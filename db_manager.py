from db_client import dbClient

import asyncio
import aiomysql
import pymysql
import traceback, warnings
import os, time
from datetime import datetime, timezone, timedelta
from decimal import Decimal, getcontext
from progress.bar import Bar

import store
from logger import log
from utils import dropTailingZeros

def terminate_check():
    filename = '{}/force_terminate'.format(os.path.abspath(os.path.dirname(__file__)))
    if os.path.exists(filename):
        return True

class dbManager(dbClient):
    def __init__(self, coin, dbConfig, worker_count):
        super().__init__(coin, dbConfig, worker_count)

    async def checkTable(self):
        # address Tx Table
        existTable = await self.dbExecute('SHOW TABLES LIKE "{}"'.format(self.tableName['address_tx']))
        if not existTable or len(existTable) == 0:
            queryString = '''CREATE TABLE `{}` (
                                `aid` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT,
                                `address` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
                                `txid` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
                                `blocknum` int(16) NOT NULL,
                                `category` int(1) NOT NULL,
                                `amount` decimal(50,20) NOT NULL,
                                PRIMARY KEY (`aid`),
                                KEY (`address`),
                                KEY (`txid`),
                                KEY (`blocknum`),
                                KEY (`category`)
                            ) ENGINE=InnoDB;'''.format(self.tableName['address_tx'])
            await self.dbExecute(queryString)

        # address Info Table
        existTable = await self.dbExecute('SHOW TABLES LIKE "{}"'.format(self.tableName['address_info']))
        if not existTable or len(existTable) == 0:
            queryString = '''CREATE TABLE `{}` (
                                `address` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
                                `lasttxnum` bigint(16) NOT NULL,
                                `txcount` int(16) NOT NULL,
                                `balance` decimal(50,20) NOT NULL,
                                PRIMARY KEY (`address`)
                            ) ENGINE=InnoDB;'''.format(self.tableName['address_info'])
            await self.dbExecute(queryString)

        existTable = await self.dbExecute('SHOW TABLES LIKE "{}"'.format(self.tableName['status']))
        if not existTable or len(existTable) == 0:
            queryString = '''CREATE TABLE `{}` (
                                `coin` text COLLATE utf8mb4_unicode_ci NOT NULL,
                                `block` int(16) NOT NULL,
                                `proctime` datetime NOT NULL,
                                `txid` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
                                `txnum` bigint(16) NOT NULL,
                                `clustering` int(16) NOT NULL,
                                PRIMARY KEY (`coin`(10))
                            ) ENGINE=InnoDB;'''.format(self.tableName['status'])
            await self.dbExecute(queryString)

        # transactionTable
        for tableName in [ self.transactionTable(num) for num in range(self.tableShardCount) ]:
            existTable = await self.dbExecute('SHOW TABLES LIKE "{}"'.format(tableName))
            if not existTable or len(existTable) == 0:
                queryString = '''CREATE TABLE `{}` (
                                      `txid` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
                                      `blocknum` int(16) NOT NULL,
                                      `txtime` datetime NOT NULL,
                                      `sender` mediumtext COLLATE utf8mb4_unicode_ci NOT NULL,
                                      `sender_amount` mediumtext COLLATE utf8mb4_unicode_ci NOT NULL,
                                      `receiver` mediumtext COLLATE utf8mb4_unicode_ci NOT NULL,
                                      `receiver_amount` mediumtext COLLATE utf8mb4_unicode_ci NOT NULL,
                                      `total_amount` decimal(50,20) NOT NULL,
                                      `memo` mediumtext COLLATE utf8mb4_unicode_ci NOT NULL,
                                      PRIMARY KEY (`txid`),
                                      KEY (`blocknum`),
                                      KEY (`sender`(50)),
                                      KEY (`receiver`(50))
                                ) ENGINE=InnoDB;'''.format(tableName)
                await self.dbExecute(queryString)

        existTable = await self.dbExecute('SHOW TABLES LIKE "{}"'.format(self.tableName['txnum']))
        if not existTable or len(existTable) == 0:
            queryString = '''CREATE TABLE `{}` (
                                  `txnum` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT,
                                  `txtime` datetime NOT NULL,
                                  `txid` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
                                  PRIMARY KEY (`txnum`),
                                  KEY (`txid`)
                            ) ENGINE=InnoDB;'''.format(self.tableName['txnum'])
            await self.dbExecute(queryString)


    async def Process(self):
        coin = self.coin
        store.data['lastStatus'][coin] = await self.getLastStatus()
        self.lastBlock = store.data['lastStatus'][coin]['block']
        lastTxnum = await self.dbExecute('SELECT txnum FROM {} ORDER BY txnum DESC LIMIT 1'.format(self.tableName['txnum']))

        if self.lastBlock > 0 and store.data['lastStatus'][coin]['txnum'] == 0:
            print('!!! {} last status is unstable (last txnum is 0)'.format(coin))
            print('!!! last txnum of {} is {}'.format(self.tableName['txnum'], lastTxnum[0][0])) 
            store.data['running'] = False
        elif len(lastTxnum) > 1 and store.data['lastStatus'][coin]['txnum'] != lastTxnum[0][0]:
            print('!!! {} last status is unstable (last txnum is different)'.format(coin))
            print('!!! last txnum of {} is {}'.format(self.tableName['status'], store.data['lastStatus'][coin]['txnum']))
            print('!!! last txnum of {} is {}'.format(self.tableName['txnum'], lastTxnum[0][0]))
            store.data['running'] = False
        else:
            print('=== start db rollback for {}'.format(coin))
            store.data['isRollbacking'] = True
            #await self.Rollback()
            store.data['isRollbacking'] = False
            print('=== rollback success')

        while store.data['running']:
            try:
                insertBuffer = store.data['buffer'][coin]['done']
                doneBlockNumbers = list(insertBuffer.keys())
                doneBlockNumbers.sort()

                for startBlockNumber in doneBlockNumbers:
                    ''' for debug 200429
                    if self.lastBlock + 1 != startBlockNumber:
                        print('=== Wait {} Block: last {}, buffer {}'.format(self.lastBlock + 1, self.lastBlock, startBlockNumber))
                        await asyncio.sleep(5)
                        break
                    '''
                     
                    if terminate_check():
                        print('Force Terminated')
                        store.data['running'] = False
                        break

                    if 'data' in insertBuffer[startBlockNumber]:
                        starttime = time.time()

                        errorCheck = True
                        data = insertBuffer.pop(startBlockNumber)
                        transdata = data['data']
                        lastBlock = startBlockNumber + data['blockCount'] - 1

                        print('=== start db processing for {} (from {} to {})'.format(coin, startBlockNumber, lastBlock))

                        if data['transCount'] > 0:
                            blocknum = [ data['blocknum'] for data in transdata ]
                            blockcount = [ (num, blocknum.count(num)) for num in list(set(blocknum)) ]
                            
                            # for debug
                            log.info('blockCount: {}'.format(blockcount))
                            log.info('TX Count: {}'.format(sum([ blks[1] for blks in blockcount ])))

                            await self.insertTransaction(transdata)
                            if coin not in ['ETH', 'ETC']:
                                errorCheck = await self.updateAddressInfo(transdata)

                        if errorCheck:
                            self.lastBlock = await self.updateLastStatus(lastBlock, self.lastTxid, self.lastTxnum)
                            log.info('===================================')
                            log.info('UPDATE LAST STATUS - BLOCK: {}, TXNUM: {}'.format(lastBlock, self.lastTxnum))
                            log.info('TXID: {}'.format(self.lastTxid))
                            log.info('===================================')
                        else:
                            print('!!! got error on updateAddressInfo')
                            log.error('!!! DONE BLOCKNUMS : {}'.format(doneBlockNumbers))
                            log.error('!!! START BLOCKNUM : {}'.format(startBlockNumber))

                            workingPath = os.path.abspath(os.path.dirname(__file__))
                            termFilename = '{}/force_terminate'.format(workingPath)
                            with open(termFilename, 'wt') as term:
                                term.write('stop')

                        processingtime = Decimal(time.time() - starttime).quantize(Decimal('.00001'))
                        print('=== db processing time {}'.format(processingtime))

            except Exception as e:
                log.error(e)
                log.error(traceback.format_exc())
            else:
                await asyncio.sleep(1)


    async def Rollback(self):
        lastStatus = store.data['lastStatus'][self.coin]

        if self.coin not in ['ETH', 'ETC']:
            return
