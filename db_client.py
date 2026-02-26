import multiprocessing
import asyncio
import aiomysql
import pymysql
import traceback, warnings
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal, getcontext

import store
from logger import log
from utils import dropTailingZeros

DB_MAX_ALLOWED_PACKET = 1024 * 1024 * 400 # max_allowed_packet, innodb_log_file_size
TX_CATEGORY_SEND = 1
TX_CATEGORY_RECEIVE = 2

class dbClient():
    def __init__(self, coin, dbConfig, worker_count):
        self.coin = coin
        self.dbConfig = dbConfig
        self.host = dbConfig['host']
        self.port = dbConfig['port']
        self.user = dbConfig['user']
        self.password = dbConfig['passwd']
        self.database = 'cctracker_{}'.format(coin)
        self.worker_count = worker_count

        self.tableName = {
            'address_tx': '{}_address_tx'.format(coin),
            'address_info': '{}_address_info'.format(coin),
            'status': '{}_status'.format(coin),
            'transaction': '{}_transaction'.format(coin),
            'cluster_master': '{}_cluster_master'.format(coin),
            'cluster_address': '{}_cluster_address'.format(coin),
            'txnum': '{}_txnum'.format(coin),
        }

        self.tableShardCount = 8

        self.printProcTime = False
        self.printProcTimeDebug = True

        self.is_init = False
        self.dbpool = None

        self.lastBlock = 0
        self.lastTxid = 0
        self.lastTxnum = 0

        self.transBuffer = []
        self.addressBuffer = []

        self.timezone = 0
        self.timezoneString = "UTC"
        self.tzdata = timezone(timedelta(hours=self.timezone))

    async def init(self):
        if not self.is_init:
            self.is_init = True
            if not self.dbpool:
                self.dbpool = await self.makeDBPool()
            await self.dbConnect()
            await self.checkTable()

    async def checkTable(self):
        raise NotImplementedError()

    async def Terminate(self):
        await self.dbDisconnect()

    def proctime(self, starttime):
        return Decimal(time.time() - starttime).quantize(Decimal('.00001'))

    def setDatabasePool(self, pool):
        self.dbpool = pool
        self.is_init = True

    # 200507 todo: before make db pool, need check db exist
    async def makeDBPool(self):
        loop = asyncio.get_event_loop()
        dbpool = await aiomysql.create_pool(minsize = int(self.worker_count / 2),
                                            maxsize = self.worker_count,
                                            host = self.host,
                                            port = self.port,
                                            user = self.user,
                                            password = self.password,
                                            db = self.database,
                                            loop = loop)
        return dbpool

    async def dbConnect(self):
        while True:
            conn = await self.dbpool.acquire()
            try:
                async with conn.cursor() as curs:
                    while True:
                        dbCheck = await curs.execute('SHOW DATABASES LIKE "{}"'.format(self.database))
                        if not dbCheck:
                            try:
                                await curs.execute('CREATE DATABASE {} DEFAULT CHARACTER SET utf8'.format(self.database))
                            except ProgrammingError as e:
                                if e.args[0] == 1007: # database exist
                                    asyncio.sleep(1)
                                    continue
                        else:
                            break
                    await curs.execute('USE {}'.format(self.database))
                    await curs.execute('SET autocommit = 1')
                    await curs.execute('SET time_zone = "{}"'.format(self.timezoneString))
            except pymysql.err.OperationalError as e:
                print(e)
                print('Connect error, retry')
                continue
            except Exception as e:
                log.error(e)
                log.error(traceback.format_exc())
                raise OSError
            else:
                self.dbpool.release(conn)
                break

    async def dbDisconnect(self):
        try:
            log.info('disconnect db {}'.format(self.dbpool))
            await asyncio.sleep(0)
            self.dbpool.close()
            await self.dbpool.wait_closed()
            self.is_init = False
        except Exception as e:
            log.error(e)
            log.error(traceback.format_exc())

    async def dbExecute(self, query, needDict = False, mustReturn = False):
        if not self.is_init:
            await self.init()

        starttime = time.time()
        result = None
        received = False

        while not received:
            data = None
            conn = None

            while not conn:
                try:
                    conn = await self.dbpool.acquire()
                except Exception as e:
                    log.error('Error on connection pool acquire - {}'.format(e))
            
            with warnings.catch_warnings():
                warnings.filterwarnings('error')
                try:
                    async with conn.cursor() as curs:
                        # for debug 200422, https://mariadb.com/kb/en/galera-cluster-system-variables/#wsrep_sync_wait
                        '''
                        await curs.execute('SET SESSION wsrep_sync_wait=7')
                        while True:
                            await curs.execute('SHOW GLOBAL STATUS LIKE "wsrep_local_state_comment"')
                            syncStatus = await curs.fetchall()
                            if syncStatus[0][0] in ['Joining', 'Waiting for SST', 'Joined']:
                                await curs.execute('SELECT SUBSTRING_INDEX(USER(), "@", -1)')
                                galeraIP = await curs.fetchall()
                                print('wait for galera syncing')
                                log.info('wait for galera syncing - {}'.format(galeraIP[0][0]))
                                await asyncio.sleep(1)
                            else:
                                break
                        '''
                        
                        # for debug 200429, https://sarc.io/index.php/mariadb/611-2016-09-07-14-33-57
                        #await curs.execute('SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE')

                        await curs.execute(query)
                        data = await curs.fetchall()

                        if mustReturn and len(data) == 0:
                            print('debug, there must be return data, but no data in the db, maybe syncing..')
                            print(query[:40])
                            print(len(query))
                            print('conn: {}'.format(conn))
                            print('curs: {}'.format(curs))
                            await asyncio.sleep(1)
                            received = False
                        else:
                            if query[:6] == 'INSERT' or query[:7] == 'REPLACE' or query[:6] == 'UPDATE':
                                await curs.execute('commit')
                            received = True
                except (pymysql.err.OperationalError, pymysql.err.InterfaceError, pymysql.err.InternalError) as e:
                    log.error('!! {}'.format(query[:77]))
                    log.error(e)

                except (pymysql.Warning, aiomysql.Warning) as e:
                    log.warning('!! {}'.format(query[:77]))
                    log.warning(e)

                except Exception as e:
                    print(e)
                    log.error(query)
                    log.error(e)
                    log.error(traceback.format_exc())
                    result = None
                else:
                    if needDict:
                        pass
                    else:
                        result = data

            self.dbpool.release(conn)

        processingtime = Decimal(time.time() - starttime).quantize(Decimal('.00001'))
        if self.printProcTime:
            printLength = 66
            queryString = query.replace('\n', ' ').replace('\t', '').replace('  ', '')[:printLength]
            space = '' if len(queryString) >= printLength else ' ' * (printLength - len(queryString))
            log.info('-- {}{} - {}'.format(queryString, space, processingtime))
        return result

    def tableNumber(self, key):
        return (int(str(key)[-1], 16) % self.tableShardCount)

    def transactionTable(self, txid):
        return '%s_%02d' % (self.tableName['transaction'], self.tableNumber(txid))


    async def updateAddressInfo(self, dataList):
        # 트랜잭션ID의 number 가져온다
        txList = {}
        for transdata in dataList:
            blockMiningTime = datetime.fromtimestamp(transdata['timestamp'], self.tzdata).strftime('%Y-%m-%d %H:%M:%S')
            txList[transdata['txid']] = blockMiningTime
        
        # for debug, 200304 db syncing delay
        txNumDict = None
        while True:
            txNumDict = await self.getTransactionsNumber(txList)
            if len(txNumDict) == len(txList):
                break


        addressTxData = {}
        addressInfo = {}

        # 한번에 넣을 address tx data, address info 병합 
        for transdata in dataList:
            txid = transdata['txid']
            blocknum = transdata['blocknum']

            # sender data
            for i in range(len(transdata['sender'])):
                sender = transdata['sender'][i]
                amount = Decimal(str(transdata['sender_amount'][i]))

                # tx data
                if sender not in addressTxData:
                    addressTxData[sender] = []

                addressTxData[sender].append({
                    'txid': txid,
                    'category': TX_CATEGORY_SEND,
                    'amount': amount,
                    'blocknum': blocknum,
                })

                # address info
                if sender not in addressInfo:
                    addressInfo[sender] = {
                        'txlist': [],
                        'balance': Decimal('0'),
                        'lasttxnum': 0,
                    }
                
                addressInfo[sender]['txlist'].append(txid)
                addressInfo[sender]['balance'] -= amount
                addressInfo[sender]['lasttxnum'] = max(addressInfo[sender]['lasttxnum'], txNumDict[txid])

            # receiver data
            for i in range(len(transdata['receiver'])):
                receiver = transdata['receiver'][i]
                amount = Decimal(str(transdata['receiver_amount'][i]))

                # tx data
                if receiver not in addressTxData:
                    addressTxData[receiver] = []

                addressTxData[receiver].append({
                    'txid': txid,
                    'category': TX_CATEGORY_RECEIVE,
                    'amount': amount,
                    'blocknum': blocknum,
                })

                # address info
                if receiver not in addressInfo:
                    addressInfo[receiver] = {
                        'txlist': [],
                        'balance': Decimal('0'),
                        'lasttxnum': 0,
                    }
                
                addressInfo[receiver]['txlist'].append(txid)
                addressInfo[receiver]['balance'] += amount
                addressInfo[receiver]['lasttxnum'] = max(addressInfo[receiver]['lasttxnum'], txNumDict[txid])

        # DB 에서 주소별 데이터 가져오고
        addressInfoDB = await self.getAddressesInfo(list(set(list(addressInfo.keys()))))

        # DB 에 있던 데이터와 트랜잭션 데이터 병합
        for address in addressInfoDB:
            addressInfoDB[address]['txcount'] += len(set(addressInfo[address]['txlist']))
            addressInfoDB[address]['balance'] += addressInfo[address]['balance']
            addressInfoDB[address]['lasttxnum'] = addressInfo[address]['lasttxnum']


        # db query - address info
        queryHeader = 'REPLACE INTO {} (address, lasttxnum, txcount, balance) VALUES '.format(self.tableName['address_info'])
        queryLength = len(queryHeader)
        queryStrings = []

        for address in addressInfoDB:
            info = addressInfoDB[address]
            queryString = '("{}",{},{},{}),'.format(address, info['lasttxnum'], info['txcount'], info['balance'])

            if queryLength + len(queryString) >= DB_MAX_ALLOWED_PACKET:
                replaceQuery = queryHeader + ''.join(queryStrings)
                replaceQuery = replaceQuery[:-1]
                await self.dbExecute(replaceQuery)

                queryLength = len(queryHeader)
                queryStrings = []

            queryLength += len(queryString)
            queryStrings.append(queryString)

        if len(queryStrings) > 0:
            replaceQuery = queryHeader + ''.join(queryStrings)
            replaceQuery = replaceQuery[:-1]
            await self.dbExecute(replaceQuery)



        # db query - address tx data
        tableName = self.tableName['address_tx']
        queryHeader = 'INSERT INTO {} (address, txid, blocknum, category, amount) VALUES '.format(tableName)
        queryLength = len(queryHeader)
        queryStrings = []

        for address in addressTxData:
            for data in addressTxData[address]:
                (txid, blocknum, category, amount) = (data['txid'], data['blocknum'], data['category'], data['amount'])
                queryString = '("{}","{}",{},{},{}),'.format(address, txid, blocknum, category, amount)

                if queryLength + len(queryString) >= DB_MAX_ALLOWED_PACKET:
                    replaceQuery = queryHeader + ''.join(queryStrings)
                    replaceQuery = replaceQuery[:-1]
                    await self.dbExecute(replaceQuery)

                    queryLength = len(queryHeader)
                    queryStrings = []

                queryLength += len(queryString)
                queryStrings.append(queryString)

        if len(queryStrings) > 0:
            replaceQuery = queryHeader + ''.join(queryStrings)
            replaceQuery = replaceQuery[:-1]
            await self.dbExecute(replaceQuery)

        return True


    async def insertTransaction(self, dataList):
        lastBlock = 0
        lastTxid = 0
        lastTxnum = 0

        starttime = time.time()
        #txList = dict.fromkeys([ transdata['txid'] for transdata in dataList ])
        txList = {}
        for transdata in dataList:
            txtime = datetime.fromtimestamp(transdata['timestamp'], self.tzdata).strftime('%Y-%m-%d %H:%M:%S')
            txList[transdata['txid']] = txtime
        txNumDict = await self.getTransactionsNumber(txList, wholeNewData = True)

        if self.printProcTimeDebug:
            log.info('-- getTransactionsNumber - {}'.format(Decimal(time.time() - starttime).quantize(Decimal('.00001'))))

        tasks = []
        dbInserters = []
        for tableSequence in list(range(self.tableShardCount)):
            dbInserter = dbWorker(self.coin, self.dbConfig, self.worker_count, self.dbpool)
            dbInserters.append(dbInserter)
            tasks.append(dbInserter.asyncInsertTransaction(dataList, tableSequence))
        await asyncio.gather(*tasks)

        '''
        tasks = []
        for dbInserter in dbInserters:
            tasks.append(dbInserter.Terminate())
        await asyncio.gather(*tasks)
        '''

        self.lastTxid = dataList[-1]['txid']
        self.lastTxnum = txNumDict[dataList[-1]['txid']]


    async def asyncInsertTransaction(self, dataList, tableSequence):
        starttime = time.time()
        tableName = self.transactionTable(tableSequence)

        insertQueryHeader = 'REPLACE INTO {} (txid, blocknum, txtime, sender, sender_amount, receiver, receiver_amount, total_amount, memo) VALUES '.format(tableName)
        packetBytes = len(insertQueryHeader)
        queryStringList = []

        #for i in list(range(len(dataList)))[tableSequence::10]:
        for transdata in dataList:
            if self.tableNumber(transdata['txid']) != tableSequence:
                continue

            memo = transdata['memo'] if 'memo' in transdata else '{}'
            sender = ','.join(transdata['sender'])
            receiver = ','.join(transdata['receiver'])
            sender_amount = ','.join([ dropTailingZeros(str(amount)) for amount in transdata['sender_amount'] ])
            receiver_amount = ','.join([ dropTailingZeros(str(amount)) for amount in transdata['receiver_amount'] ])
            txtime = datetime.fromtimestamp(transdata['timestamp'], self.tzdata).strftime('%Y-%m-%d %H:%M:%S') # TODO !!!!!!!!!!!!!!! timezone 처리 고민, 현재 DB 에는 UTC 로 저장하고 있음

            queryString = '("{}", {}, "{}", "{}", "{}", "{}", "{}", {}, \'{}\'),'.format(transdata['txid'],
                                                                                            transdata['blocknum'],
                                                                                            txtime,
                                                                                            sender,
                                                                                            sender_amount,
                                                                                            receiver,
                                                                                            receiver_amount,
                                                                                            transdata['total_amount'],
                                                                                            memo)
            packetBytes += len(queryString)

            if packetBytes >= DB_MAX_ALLOWED_PACKET:
                insertTransQuery = insertQueryHeader + ' '.join(queryStringList)
                insertTransQuery = insertTransQuery[:-1]
                await self.dbExecute(insertTransQuery)

                packetBytes = len(insertQueryHeader)
                queryStringList = []

            queryStringList.append(queryString)

        if len(queryStringList) > 0:
            insertTransQuery = insertQueryHeader + ' '.join(queryStringList)
            insertTransQuery = insertTransQuery[:-1]
            await self.dbExecute(insertTransQuery)


        if self.printProcTimeDebug:
            log.info('-- insertQuery - {} / {}'.format(tableSequence, Decimal(time.time() - starttime).quantize(Decimal('.00001'))))


    async def getAddressesTx(self, addresses):
        tableName = self.tableName['address_tx']
        neededColumns = ('address', 'txid', 'category', 'amount')
        addressKey = '"{}"'.format('","'.join(addresses))
        queryString = 'SELECT {} FROM {} WHERE address in ({})'.format(','.join(neededColumns), tableName, addressKey)
        addressesTxFromDB = await self.dbExecute(queryString)
        addressesTx = {}

        try:
            for data in addressesTxFromDB:
                addressesTx[data[0]] = {}
                for i in range(1, len(neededColumns)):
                    addressesTx[data[0]][neededColumns[i]] = data[i]

        except Exception as e:
            log.error(addressesTxFromDB)
            log.error(e)
            log.error(traceback.format_exc())

        return addressesTx


    async def getAddressesInfo(self, addresses):
        tableName = self.tableName['address_info']
        neededColumns = ('address', 'lasttxnum', 'txcount', 'balance')
        addressKey = '"{}"'.format('","'.join(addresses))

        queryString = 'SELECT {} FROM {} WHERE address in ({})'.format(','.join(neededColumns), tableName, addressKey)
        addressesInfoFromDB = await self.dbExecute(queryString)
        addressesInfo = {}

        try:
            for info in addressesInfoFromDB:
                addressesInfo[info[0]] = {}
                for i in range(1, len(neededColumns)):
                    addressesInfo[info[0]][neededColumns[i]] = info[i]
        except Exception as e:
            log.error(addressesInfoFromDB)
            log.error(e)
            log.error(traceback.format_exc())

        for address in addresses:
            if address not in addressesInfo:
                addressesInfo[address] = {'lasttxnum': 0, 'txcount': 0, 'balance': Decimal('0')}

        return addressesInfo


    async def getTransactionsNumber(self, txids, wholeNewData = False):
        txIdList = txids
        if isinstance(txids, dict):
            txIdList = list(txids.keys())

        nonexist = []
        transactionsNumber = {}

        if not wholeNewData:
            queryString = '''SELECT txid, txnum FROM {}
                              WHERE txid in ("{}")
                              ORDER BY txnum ASC'''.format(self.tableName['txnum'], '","'.join(txIdList))
            transactionsNumberFromDB = await self.dbExecute(queryString)
            transactionsNumber = { data[0]: data[1] for data in transactionsNumberFromDB } # TXID: TXNUM

            for txid in txids:
                if txid not in transactionsNumber:
                    nonexist.append(txid)
        else:
            nonexist = txIdList

        if 0 < len(nonexist):
            if self.printProcTimeDebug:
                log.info('-- length of nonexist - {}'.format(len(nonexist)))

            queryString = ''
            try:
                queryString = 'INSERT INTO {} (txid, txtime) VALUES ("{}", "{}")'.format(self.tableName['txnum'], nonexist[0], txids[nonexist[0]])
            except Exception as e:
                print('-' * 20)
                print('nonexist:')
                print(nonexist)
                print('txids:')
                print(txids)
                print('-' * 20)
                print(e)
                print(traceback.format_exc())

            await self.dbExecute(queryString)
            #queryString = 'SELECT LAST_INSERT_ID()'
            queryString = 'SELECT txnum FROM {} ORDER BY txnum DESC LIMIT 1'.format(self.tableName['txnum'])
            firstTxnum = await self.dbExecute(queryString)
            firstTxnum = int(firstTxnum[0][0])

            received = True
            if 2 < len(nonexist):
                queryString = 'INSERT INTO {} (txid, txtime) VALUES '.format(self.tableName['txnum'])
                for txid in nonexist[1:-1]:
                    queryString += '("{}", "{}"), '.format(txid, txids[txid])
                queryString = queryString[:-2]
                received = False
                await self.dbExecute(queryString)
                received = True

            if 1 < len(nonexist):
                if not wholeNewData:
                    while not received:
                        print('debug - wating for insert txid')
                        await asyncio.sleep(1)

                    queryString = '''SELECT txid, txnum FROM {} WHERE txnum >= "{}"
                                        AND txid in ("{}")
                                      ORDER BY txnum ASC'''.format(self.tableName['txnum'], firstTxnum, '","'.join(nonexist))
                    transactionsNumberFromDB = await self.dbExecute(queryString, mustReturn = True)
                    transactionsNumber = dict(transactionsNumber, **{ data[0]: data[1] for data in transactionsNumberFromDB })

                    '''
                    log.info('for debug')
                    log.info('queryString')
                    log.info(queryString)
                    log.info('nonexist')
                    log.info('{}'.format(nonexist))
                    log.info('transactionsNumberFromDB')
                    log.info('{}'.format(transactionsNumberFromDB))
                    log.info('transactionsNumber')
                    log.info('{}'.format(transactionsNumber))
                    '''

                else:
                    queryString = 'INSERT INTO {} (txid, txtime) VALUES ("{}", "{}")'.format(self.tableName['txnum'], nonexist[-1], txids[nonexist[-1]])
                    await self.dbExecute(queryString)
                    #queryString = 'SELECT LAST_INSERT_ID()'
                    queryString = 'SELECT txnum FROM {} ORDER BY txnum DESC LIMIT 1'.format(self.tableName['txnum'])
                    lastTxnum = await self.dbExecute(queryString)
                    lastTxnum = int(lastTxnum[0][0])
                    transactionsNumber[nonexist[-1]] = lastTxnum

        return transactionsNumber


    async def getTransactionIds(self, txnum):
        txnum_ = list(map(str, txnum))
        queryString = 'SELECT txnum, txid FROM {} WHERE txnum in ("{}")'.format(self.tableName['txnum'], '","'.join(txnum_))
        transactionsNumberFromDB = await self.dbExecute(queryString)
        transactionsNumber = { data[0]: data[1] for data in transactionsNumberFromDB } # TXNUM: TXID
        return transactionsNumber


    async def getTransactionDataOld(self, start, count):
        queryString = 'SELECT txid, txtime, blocknum, sender, receiver, sender_amount, total_amount FROM {} WHERE blocknum BETWEEN {} and {} and sender <> "Mining" ORDER BY blocknum ASC'.format(self.tableName['transaction'], start, start + count - 1)
        transDatasFromDB = await self.dbExecute(queryString)
        transDatas = {}

        for transData in transDatasFromDB:
            (txid, txtime, blocknum, sender, receiver, sender_amount, total_amount) = transData
            transDatas[txid] = {}
            transDatas[txid]['sender'] = sender.split(',')
            transDatas[txid]['receiver'] = receiver.split(',')
            transDatas[txid]['sender_amount'] = sum(map(Decimal, sender_amount.split(',')))
            transDatas[txid]['receiver_amount'] = Decimal(str(total_amount))
            transDatas[txid]['blocknum'] = blocknum
            transDatas[txid]['txtime'] = txtime

        return transDatas


    async def getTransactionDataFromTxid(self, txids):
        transDatas = {}

        tasks = []
        dbSelectors = []
        for tableSequence in list(range(self.tableShardCount)):
            dbSelector = dbWorker(self.coin, self.dbConfig, self.worker_count, self.dbpool)
            dbSelectors.append(dbSelector)
            tasks.append(dbSelector.asyncGetTransactionDataFromTxid(txids, tableSequence))
        
        asyncDatas = await asyncio.gather(*tasks)
        for data in asyncDatas:
            transDatas = dict(transDatas, **data)

        return transDatas


    async def asyncGetTransactionDataFromTxid(self, txids, tableSequence):
        starttime = time.time()
        tableName = self.transactionTable(tableSequence)
        transDatas = {}

        filtedTxids = []
        for txid in txids:
            if self.tableNumber(txid) == tableSequence:
                filtedTxids.append(txid)

        queryString = '''SELECT txid, txtime, blocknum, sender, receiver, sender_amount, receiver_amount, total_amount
                           FROM {}
                          WHERE txid in ("{}")'''.format(tableName, '","'.join(filtedTxids))
        transDatasFromDB = await self.dbExecute(queryString)

        for transData in transDatasFromDB:
            (txid, txtime, blocknum, sender, receiver, sender_amount, receiver_amount, total_amount) = transData
            transDatas[txid] = {}
            transDatas[txid]['sender'] = sender.split(',')
            transDatas[txid]['receiver'] = receiver.split(',')
            transDatas[txid]['sender_amount'] = sum(map(Decimal, sender_amount.split(',')))
            transDatas[txid]['receiver_amount'] = sum(map(Decimal, receiver_amount.split(',')))
            transDatas[txid]['total_amount'] = Decimal(str(total_amount))
            transDatas[txid]['blocknum'] = blocknum
            transDatas[txid]['txtime'] = txtime


        if self.printProcTimeDebug:
            log.info('-- selectQuery - {} / {}'.format(tableSequence, Decimal(time.time() - starttime).quantize(Decimal('.00001'))))

        return transDatas


    async def getTransactionData(self, start, count):
        transDatas = {}
        for tableName in [ self.transactionTable(n) for n in list(range(start, start + count))[:self.tableShardCount] ]:
            queryString = '''SELECT txid, txtime, blocknum, sender, receiver, sender_amount, total_amount
                               FROM {}
                              WHERE blocknum BETWEEN {} and {} and sender <> "Mining"
                              ORDER BY blocknum ASC'''.format(tableName, start, start + count - 1)
            transDatasFromDB = await self.dbExecute(queryString)

            for transData in transDatasFromDB:
                (txid, txtime, blocknum, sender, receiver, sender_amount, total_amount) = transData
                transDatas[txid] = {}
                transDatas[txid]['sender'] = sender.split(',')
                transDatas[txid]['receiver'] = receiver.split(',')
                transDatas[txid]['sender_amount'] = sum(map(Decimal, sender_amount.split(',')))
                ## 200721 키는 receiver, 밸류는 total 임.. 확인요
                transDatas[txid]['receiver_amount'] = Decimal(str(total_amount))
                transDatas[txid]['blocknum'] = blocknum
                transDatas[txid]['txtime'] = txtime

        return transDatas


    ##################
    ### for clustering
    ##################

    async def getAddressData(self, addresses):
        # addressDatas = {address: (cid, first_timestamp, last_timestamp), ...}
        addressDatas = {}

        starttime = time.time()
        queryString = 'SELECT address, blocknum, txid FROM {} WHERE address in ("{}")'.format(self.tableName['address_tx'], '","'.join(addresses))
        addressDatasFromDB = await self.dbExecute(queryString)
        if self.printProcTimeDebug:
            log.info('-- select from address_tx - {}'.format(Decimal(time.time() - starttime).quantize(Decimal('.00001'))))

        starttime = time.time()
        addressTxids = {}
        for address, blocknum, txid in addressDatasFromDB:
            if address not in addressTxids:
                addressTxids[address] = {'firstblock': None, 'lastblock': None, 'first_txid': [], 'last_txid': []}
            
            if not addressTxids[address]['firstblock'] or addressTxids[address]['firstblock'] == blocknum:
                # 200721 block에 포함된 tx data 의 time 이 동일하므로, 하나만 선택해도 시간을 구하는데는 오차가 없음
                #addressTxids[address]['first_txid'].append(txid)
                addressTxids[address]['first_txid'] = [txid]
                addressTxids[address]['firstblock'] = blocknum
            elif addressTxids[address]['firstblock'] > blocknum:
                addressTxids[address]['first_txid'] = [txid]
                addressTxids[address]['firstblock'] = blocknum

            if not addressTxids[address]['lastblock'] or addressTxids[address]['lastblock'] == blocknum:
                # 200721 block에 포함된 tx data 의 time 이 동일하므로, 하나만 선택해도 시간을 구하는데는 오차가 없음
                #addressTxids[address]['last_txid'].append(txid)
                addressTxids[address]['last_txid'] = [txid]
                addressTxids[address]['lastblock'] = blocknum
            elif addressTxids[address]['lastblock'] < blocknum:
                addressTxids[address]['last_txid'] = [txid]
                addressTxids[address]['lastblock'] = blocknum

        if self.printProcTimeDebug:
            log.info('-- make addressTxids data - {}'.format(Decimal(time.time() - starttime).quantize(Decimal('.00001'))))
        

        starttime = time.time()
        filtedTxids = []
        for address in addressTxids:
            filtedTxids.extend(addressTxids[address]['first_txid'])
            filtedTxids.extend(addressTxids[address]['last_txid'])
        filtedTxids = list(set(filtedTxids))
        if self.printProcTimeDebug:
            log.info('-- make filtedTxids data - {}'.format(Decimal(time.time() - starttime).quantize(Decimal('.00001'))))

        
        starttime = time.time()
        transDatas = await self.getTransactionDataFromTxid(filtedTxids)
        # 200721 block에 포함된 tx data 의 time 이 동일하지 않다면, 시간 순으로 정렬해서 최초, 최종시간 해당 txid 만 필터링해야함
        if self.printProcTimeDebug:
            log.info('-- getTransactionDataFromTxid - {}'.format(Decimal(time.time() - starttime).quantize(Decimal('.00001'))))

        starttime = time.time()
        # Initialize addressDatas
        for address in addressTxids:
            cid = None

            # 200721 block에 포함된 tx data 의 time 이 동일하므로, txid list 에서는 element 가 1개만 있음
            first_txid = addressTxids[address]['first_txid'][0]
            last_txid = addressTxids[address]['last_txid'][0]
            first_timestamp = int(time.mktime(transDatas[first_txid]['txtime'].timetuple()))
            last_timestamp = int(time.mktime(transDatas[last_txid]['txtime'].timetuple()))
            
            addressDatas[address] = (cid, first_timestamp, last_timestamp)
        if self.printProcTimeDebug:
            log.info('-- initialize addressDatas - {}'.format(Decimal(time.time() - starttime).quantize(Decimal('.00001'))))
        
        starttime = time.time()
        queryString = 'SELECT address, cid FROM {} WHERE address in ("{}")'.format(self.tableName['cluster_address'], '","'.join(addresses))
        clusterDatasFromDB = await self.dbExecute(queryString)
        if self.printProcTimeDebug:
            log.info('-- select cluster_address - {}'.format(Decimal(time.time() - starttime).quantize(Decimal('.00001'))))

        starttime = time.time()
        # modifying addressDatas from DB
        for clusterData in clusterDatasFromDB:
            (address, cid) = clusterData
            cid = cid if cid > 0 else None # init clusterId
            first_timestamp = addressDatas[address][1]
            last_timestamp = addressDatas[address][2]

            addressDatas[address] = (cid, first_timestamp, last_timestamp)
        if self.printProcTimeDebug:
            log.info('-- modifying addressDatas - {}'.format(Decimal(time.time() - starttime).quantize(Decimal('.00001'))))

        return addressDatas

    async def makeClusterMaster(self, address, first_txtime, last_txtime):
        first_txtime_ = datetime.fromtimestamp(first_txtime, self.tzdata).strftime('%Y-%m-%d %H:%M:%S')
        last_txtime_ = datetime.fromtimestamp(last_txtime, self.tzdata).strftime('%Y-%m-%d %H:%M:%S')
        queryString = 'INSERT INTO {} (isAlive, first_txtime, last_txtime, master) VALUES (1, "{}", "{}", "{}")'.format(self.tableName['cluster_master'], first_txtime_, last_txtime_, address)
        await self.dbExecute(queryString)
        
        queryString = 'SELECT cid FROM {} ORDER BY cid DESC LIMIT 1'
        clusterIdFromDB = await self.dbExecute(queryString)
        cid = clusterIdFromDB[0][0]

        queryString = 'INSERT INTO {} (address, cid) VALUES ("{}", {})'.format(self.tableName['cluster_address'], address, cid)
        await self.dbExecute(queryString)

        return cid


    async def makeClusterMasterList(self, addressData):
        # addressData = {address: (cid, first_timestamp, last_timestamp), ...}
        addresses = [address for address in list(addressData.keys()) if not addressData[address][0]]

        if addresses:
            queryString = 'INSERT INTO {} (isAlive, first_txtime, last_txtime, master) VALUES '.format(self.tableName['cluster_master'])
            for address in addresses:
                first_txtime = datetime.fromtimestamp(addressData[address][1], self.tzdata).strftime('%Y-%m-%d %H:%M:%S')
                last_txtime = datetime.fromtimestamp(addressData[address][2], self.tzdata).strftime('%Y-%m-%d %H:%M:%S')
                queryString += '(1, "{}", "{}", "{}"), '.format(first_txtime, last_txtime, address)
            queryString = queryString[:-2]
            await self.dbExecute(queryString)

            queryString = 'SELECT cid, master FROM {} WHERE master in ("{}")'.format(self.tableName['cluster_master'], '","'.join(addresses))

            # 200722 syncing delay temp code
            while True:
                cidDatasFromDB = await self.dbExecute(queryString)
                cidDatas = { address: cid for cid, address in cidDatasFromDB }

                if len(cidDatas) == len(addresses):
                    break

            queryString = 'INSERT INTO {} (address, cid) VALUES '.format(self.tableName['cluster_address'])
            for address in addresses:
                cid = cidDatas[address]
                first_timestamp = addressData[address][1]
                last_timestamp = addressData[address][2]
                addressData[address] = (cid, first_timestamp, last_timestamp)

                queryString += '("{}", {}), '.format(address, cid)
            queryString = queryString[:-2]
            await self.dbExecute(queryString)

        return addressData


    async def getClusterMaster(self, clusterIds):
        # clusterMaster = {cid: (first_timestamp, last_timestamp, master), ...}
        clusterMaster = {}
        if len(clusterIds) > 0:
            queryString = 'SELECT cid, first_txtime, last_txtime, master FROM {} WHERE isAlive = 1 and cid in ({})'.format(self.tableName['cluster_master'], ','.join([str(n) for n in clusterIds]))
            clusterMasterFromDB = await self.dbExecute(queryString)

            # 200903 syncing delay temp code len( ((),) ) = 1
            while len(clusterMasterFromDB) != len(clusterIds) or (len(clusterMasterFromDB) == 1 and len(clusterMasterFromDB[0]) == 0):
                clusterMasterFromDB = await self.dbExecute(queryString)
                await asyncio.sleep(0)

            clusterMaster = { data[0]: (int(time.mktime(data[1].timetuple())), int(time.mktime(data[2].timetuple())), data[3]) for data in clusterMasterFromDB }

        return clusterMaster


    async def setClusterMaster(self, clusterMaster):
        # clusterMaster = {cid: (first_timestamp, last_timestamp, master), ...}
        for cid in clusterMaster:
            first_txtime = datetime.fromtimestamp(clusterMaster[cid][0], self.tzdata).strftime('%Y-%m-%d %H:%M:%S')
            last_txtime = datetime.fromtimestamp(clusterMaster[cid][1], self.tzdata).strftime('%Y-%m-%d %H:%M:%S')
            master = clusterMaster[cid][2]
            queryString = 'REPLACE INTO {} (cid, isAlive, first_txtime, last_txtime, master) VALUES ({}, 1, "{}", "{}", "{}")'.format(self.tableName['cluster_master'], cid, first_txtime, last_txtime, master)
            await self.dbExecute(queryString)


    async def setClusterData(self, clusterData):
        # clusterData = {cid: [address1, address2, ...], ...}
        for cid in clusterData:
            queryString = 'UPDATE {} SET cid = {} WHERE address IN ("{}")'.format(self.tableName['cluster_address'], cid, '","'.join(clusterData[cid]))
            await self.dbExecute(queryString)


    async def mergeClusterData(self, setClusterId, targetClusterIds):
        cids = [str(n) for n in targetClusterIds]
        queryString = 'UPDATE {} SET cid = {} WHERE cid IN ({})'.format(self.tableName['cluster_address'], setClusterId, ','.join(cids))
        await self.dbExecute(queryString)

        queryString = 'UPDATE {} SET isAlive = 0 WHERE cid in ({})'.format(self.tableName['cluster_master'], ','.join(cids))
        await self.dbExecute(queryString)

    '''
    async def mergeClusterData2(self, mergeData):
        # mergeData = {cid: [targetCids...], ...}
        for cid in mergeData:
            cids = [str(n) for n in mergeData[cid]]
            queryString = 'UPDATE {} SET cid = {} WHERE cid IN ({})'.format(self.tableName['address'], cid, ','.join(cids))
            await self.dbExecute(queryString)

        cids = []
        for cid in mergeData:
            cids.extend(mergeData[cid])
        cids = [str(n) for n in list(set(cids))]
        queryString = 'UPDATE {} SET isAlive = 0 WHERE cid in ({})'.format(self.tableName['clustering'], ','.join(cids))
        await self.dbExecute(queryString)
    '''

    async def setClusterStatus(self, lastBlocknum):
        queryString = 'UPDATE {} SET clustering = {} WHERE coin = "{}"'.format(self.tableName['status'], lastBlocknum, self.coin)
        await self.dbExecute(queryString)


    async def getClusterStatus(self):
        queryString = 'SELECT clustering FROM {} WHERE coin = {}'.format(self.tableName['status'], self.coin)
        lastBlocknum = await self.dbExecute(queryString)
        return lastBlocknum

    ########################
    ### end _ for clustering
    ########################


    async def getLastStatus(self):
        existTable = await self.dbExecute('SHOW TABLES LIKE "{}"'.format(self.tableName['status']))
        while not existTable or len(existTable) == 0:
            await asyncio.sleep(1)
            existTable = await self.dbExecute('SHOW TABLES LIKE "{}"'.format(self.tableName['status']))

        queryString = 'SELECT * FROM {} WHERE coin = "{}"'.format(self.tableName['status'], self.coin)
        dataCheck = await self.dbExecute(queryString)
        if 0 == len(dataCheck):
            tzdata = timezone(timedelta(hours=9))
            proctime = datetime.fromtimestamp(0, tzdata).strftime('%Y-%m-%d %H:%M:%S')
            queryString = 'INSERT INTO {} (coin, block, proctime, txid, txnum, clustering) VALUES ("{}", 0, "{}", "", 0, 0)'.format(self.tableName['status'], self.coin, proctime)
            await self.dbExecute(queryString)

        queryString = 'SELECT block, proctime, txid, txnum, clustering FROM {} WHERE coin = "{}"'.format(self.tableName['status'], self.coin)
        statusData = await self.dbExecute(queryString)
        (block, proctime, txid, txnum, clustering) = statusData[0]

        lastStatus = {}
        lastStatus['block'] = block
        lastStatus['proctime'] = proctime
        lastStatus['txid'] = txid
        lastStatus['txnum'] = txnum
        lastStatus['clustering'] = clustering
        return lastStatus


    async def updateLastStatus(self, blocknum, txid, txnum):
        tzdata = timezone(timedelta(hours=9))
        proctime = datetime.fromtimestamp(time.time(), tzdata).strftime('%Y-%m-%d %H:%M:%S')
        queryString = 'UPDATE {} SET block = {}, proctime = "{}", txid = "{}", txnum = {} WHERE coin = "{}"'.format(self.tableName['status'], blocknum, proctime, txid, txnum, self.coin)
        await self.dbExecute(queryString)

        return blocknum


class dbWorker(dbClient):
    def __init__(self, coin, dbConfig, worker_count, dbpool):
        super().__init__(coin, dbConfig, worker_count)
        self.setDatabasePool(dbpool)

    async def checkTable(self):
        pass
