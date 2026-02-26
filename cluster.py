from db_client import dbClient
import asyncio
from concurrent import futures
import multiprocessing
import traceback
import time
import numpy
import store
import copy
import json
from copy import deepcopy
from decimal import Decimal
from logger import log

from pprint import pprint


ADDRTYPE_LEGACY = 0
ADDRTYPE_SEGWIT = 1
ADDRTYPE_BECH32 = 2
ADDRTYPE_CONTRACT = 3
ADDRTYPE_MULTISIG = 4
ADDRTYPE_UNKNOWN = 5
ADDRTYPE_PUBKEYHASH = 6

MAKE_NEW_CLUSTERID = False

class Cluster(dbClient):
    def __init__(self, coin, dbConfig, worker_count):
        super().__init__(coin, dbConfig, worker_count)

        self.coin = coin
        self.printProcTime = False

        self.sleepSec = 10
        self.blockPartial = 100

        self.makeNewClusterId = MAKE_NEW_CLUSTERID

    def addressType(self, address):
        raise NotImplementedError()

    async def checkTable(self):
        clusteringData = [
            (
                self.tableName['cluster_master'],
                '''CREATE TABLE `{}` (
                        `cid` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT,
                        `isAlive` tinyint(1) NOT NULL DEFAULT '1',
                        `first_txtime` datetime NOT NULL,
                        `last_txtime` datetime NOT NULL,
                        `master` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
                        PRIMARY KEY (`cid`),
                        KEY (`master`),
                        KEY (`isAlive`)
                ) ENGINE=InnoDB;'''.format(self.tableName['cluster_master'])
            ),
            (
                self.tableName['cluster_address'],
                '''CREATE TABLE `{}` (
                        `aid` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT,
                        `address` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
                        `cid` int(10) NOT NULL,
                        PRIMARY KEY (`aid`),
                        KEY (`address`),
                        KEY (`cid`)
                ) ENGINE=InnoDB;'''.format(self.tableName['cluster_address'])
            )
        ]

        for tableName, queryString in clusteringData:
            existTable = await self.dbExecute('SHOW TABLES LIKE "{}"'.format(tableName))
            if self.makeNewClusterId:
                await self.dbExecute('UPDATE {} SET cid = -1'.format(tableName))
            if len(existTable) == 1 and self.makeNewClusterId:
                await self.dbExecute('DROP TABLE {}'.format(tableName))
            if len(existTable) == 0 or (len(existTable) == 1 and self.makeNewClusterId):
                await self.dbExecute(queryString)


    async def Process(self):
        await self.init()

        while store.data['running']:
            lastStatus = await self.getLastStatus()
            latestBlock = lastStatus['block']
            lastCluster = lastStatus['clustering']
            startBlock = lastCluster + 1
            blockCount = self.blockPartial

            if lastCluster == latestBlock:
                #print('[CLUSTERING {}] sleep {} secs'.format(self.coin, self.sleepSec))
                await asyncio.sleep(self.sleepSec)
                continue

            print('[CLUSTERING {}] status {} / {}'.format(self.coin, lastCluster, latestBlock))
            while startBlock < latestBlock:
                if startBlock + blockCount > latestBlock:
                    blockCount = latestBlock - startBlock + 1

                await self.Clustering(startBlock, blockCount)
                startBlock += blockCount

                while True:
                    checkData_1 = await self.dbExecute('select cid, master from BTC_cluster_master where isAlive = 1')
                    checkData_2 = await self.dbExecute('select cid, count(cid) from BTC_cluster_address group by cid')
                    master_len = len(checkData_1)
                    address_len = len(checkData_2)

                    if master_len == address_len:
                        break
                    else:
                        print('wait db syncing, master: {} address: {}'.format(master_len, address_len))
                        await asyncio.sleep(1)

                '''
                if startBlock == 3101 or startBlock > 5100:
                    store.data['running'] = False
                    break
                '''

    async def Clustering(self, startBlock, blockCount):
        starttime = time.time()

        partstarttime = time.time()
        transDatas = await self.getTransactionData(startBlock, blockCount)
        log.info('get transaction {}'.format(Decimal(time.time() - partstarttime).quantize(Decimal('.00001'))))

        print('[CLUSTERING {}] {} to {} / tx length {}'.format(self.coin, startBlock, startBlock + blockCount - 1, len(transDatas)))

        tempDebugging = False

        while len(transDatas) > 0:
            transDataPartial = {}
            for txid in list(transDatas.keys())[:10000]:
                transDataPartial[txid] = transDatas.pop(txid)

            try:
                senderList = []
                receiverList = []
                for txid in transDataPartial:
                    senderList.extend(transDataPartial[txid]['sender'])
                    receiverList.extend(transDataPartial[txid]['receiver'])

                if tempDebugging:
                    print('senderList')
                    pprint(list(set(senderList)))
                    print('receiverList')
                    pprint(list(set(receiverList)))

                partstarttime = time.time()
                # {address: (cid, first_timestamp, last_timestamp) // (first_timestamp, cid), ...}
                addressData = await self.getAddressData(list(set(senderList)))
                receiverData = await self.getAddressData(list(set(receiverList)))
                log.info('getAddressData - {}'.format(Decimal(time.time() - partstarttime).quantize(Decimal('.00001'))))
                miningData = {}

                if tempDebugging:
                    print('addressData')
                    pprint(addressData)
                    print('receiverData')
                    pprint(receiverData)

                partstarttime = time.time()
                addressData = await self.makeClusterMasterList(addressData)

                if tempDebugging:
                    print('addressData after makeClusterMasterList')
                    pprint(addressData)

                clusterData = self.calcClusterData(addressData) # {cid: [address1, address2, ...], ...}
                
                if tempDebugging:
                    print('clusterData')
                    pprint(clusterData)

                clusterMaster = await self.getClusterMaster(clusterData.keys()) # {cid: (first_timestamp, last_timestamp, master), ...}
                
                if tempDebugging:
                    print('clusterMaster')
                    pprint(clusterMaster)

                log.info('preparing base data {}'.format(Decimal(time.time() - partstarttime).quantize(Decimal('.00001'))))

                lastBlocknum = 0

                worker_varlist = []
                transDataPartialKeys = [list(l) for l in numpy.array_split(list(set(transDataPartial.keys())), self.worker_count)]

                partstarttime = time.time()
                for txidList in transDataPartialKeys:
                    for txid in txidList:
                        transData = transDataPartial[txid]
                        txtime = transData['txtime']
                        sender = transData['sender']
                        receiver = transData['receiver']
                        isMining = transData['sender_amount'] < transData['receiver_amount']

                        ####### mining data clustering
                        if isMining:
                            cid = addressData[sender[0]][0]
                            if cid not in miningData:
                                miningData[cid] = []
                            miningData[cid].extend(receiver)
                            miningData[cid] = sorted(set(miningData[cid]))

                            if tempDebugging and False:
                                print('Found Mining Data')
                                print('txid', txid)
                                print('cid', cid)
                                print('receiver', miningData[cid])
                                print('=========')
                        ##############################

                        ########### Change returns data clustering
                        '''
                        newAddress = None
                        newAddressCount = 0
                        for address in receiver:
                            if not address or '' == address:
                                print(transData)
                            if receiverData[address][1] == int(time.mktime(txtime.timetuple())):
                                newAddressCount += 1
                                newAddress = address

                        if newAddressCount == 1:
                            cid = addressData[sender[0]][0]
                            clusterData[cid].extend(receiver)
                            clusterData[cid] = sorted(set(clusterData[cid]))

                            print('Found Change Address')
                            print('txid', txid)
                            print('cid', cid)
                            print('receiver', clusterData[cid])
                            print('=========')
                        '''
                        ##########################################

                        txAddressData = { address: value for address, value in addressData.items() if address in sender }
                        txClusterData = {}
                        for cid in sorted(set([txAddressData[address][0] for address in txAddressData])):
                            txClusterData[cid] = copy.deepcopy(clusterData[cid])  ## todo 여기서 딥카피를 해주고

                        worker_varlist.append((self.coin, txid, txAddressData, txClusterData))
                log.info('making partial data {}'.format(Decimal(time.time() - partstarttime).quantize(Decimal('.00001'))))

                partstarttime = time.time()
                log.info('starting ProcessPoolExecutor - commonSpending {}'.format(Decimal(time.time() - starttime).quantize(Decimal('.00001'))))

                newClusterDataList = []
                with futures.ProcessPoolExecutor(max_workers=self.worker_count) as executor:
                    future = executor.map(Cluster.commonSpending, *zip(*worker_varlist))
                    newClusterDataList = list(future)
                log.info('commonSpending {}'.format(Decimal(time.time() - partstarttime).quantize(Decimal('.00001'))))

                partstarttime = time.time()
                newClusterData = self.mergeClusterProcessingData(newClusterDataList)
                log.info('mergeClusterProcessingData - future works {}'.format(Decimal(time.time() - partstarttime).quantize(Decimal('.00001'))))

                if tempDebugging:
                    print('clusterData before update')
                    pprint(clusterData)
                    print('newClusterData')
                    pprint(newClusterData)

                # update cluster data
                clusterData.update(newClusterData['cluster'])

                if tempDebugging:
                    print('clusterData after update')
                    pprint(clusterData)

                # cluster 데이터 체크해서 병합할 cid 걸러내고 merge 리스트 생성
                for masterCid in newClusterData['cluster']:
                    for targetAddress in newClusterData['cluster'][masterCid]:
                        mergeList = []
                        for mergeCid in [cid for cid in clusterData.keys() if cid != masterCid]:
                            if targetAddress in clusterData[mergeCid]:
                                mergeList.append(mergeCid)

                        if len(mergeList) > 0:
                            if masterCid not in newClusterData['merge']:
                                newClusterData['merge'][masterCid] = []
                            newClusterData['merge'][masterCid].extend(mergeList)

                    if masterCid in newClusterData['merge']:
                        newClusterData['merge'][masterCid] = list(set(newClusterData['merge'][masterCid]))

                if tempDebugging:
                    print('newClusterData filtering cid and make merge data')
                    pprint(newClusterData)

                # miningData 를 merge 데이터에 병합
                for masterCid in miningData:
                    for targetAddress in miningData[masterCid]:
                        targetData = None
                        if targetAddress in addressData:
                            targetData = addressData[targetAddress]
                        if not targetData and targetAddress in receiverData:
                            targetData = receiverData[targetAddress]

                        if targetData:
                            clusterData[masterCid].append(targetAddress)
                            if masterCid != targetData[0]:
                                if masterCid not in newClusterData['merge']:
                                    newClusterData['merge'][masterCid] = []
                                newClusterData['merge'][masterCid].append(targetData[0])

                    if masterCid in clusterData:
                        clusterData[masterCid] = list(set(clusterData[masterCid]))

                if tempDebugging:
                    print('clusterData merge miningData')
                    pprint(clusterData)
                    print('newClusterData merge miningData')
                    pprint(newClusterData)

                partstarttime = time.time()
                newClusterData = self.mergeClusterProcessingData(newClusterData)
                log.info('mergeClusterProcessingData {}'.format(self.proctime(partstarttime)))

                # merge 데이터 병합
                for masterCid in newClusterData['merge']:
                    if len(newClusterData['merge'][masterCid]) > 0:
                        for mergeCid in [cid for cid in newClusterData['merge'][masterCid] if cid != masterCid]:
                            if mergeCid in clusterData:
                                clusterData[masterCid].extend(clusterData.pop(mergeCid))
                            else:
                                mergeData = self.calcClusterData(receiverData)
                                clusterData[masterCid].extend(mergeData[mergeCid])
                    if masterCid in clusterData:
                        clusterData[masterCid] = list(set(clusterData[masterCid]))

                if tempDebugging:
                    print('newClusterData after processing')
                    pprint(newClusterData)
                    print('clusterData after processing')
                    pprint(clusterData)

                # make new cluster id, insert db
                for data in newClusterData['new']:
                    cid = await self.makeClusterMaster(data[0], data[2], data[3])
                    clusterData[cid] = data[1]

                # merge cluster data to db
                for masterCid in newClusterData['merge']:
                    if len(newClusterData['merge'][masterCid]) > 0:
                        await self.mergeClusterData(masterCid, newClusterData['merge'][masterCid])


                addressData = self.updateAddressData(addressData, clusterData)
                lastBlocknum = transData['blocknum']

                clusterMaster = self.updateClusterMaster(clusterMaster, addressData)

                if tempDebugging:
                    print('addressData after updateAddressData')
                    pprint(addressData)
                    print('clusterMaster after updateClusterMaster')
                    pprint(clusterMaster)


                partstarttime = time.time()
                await self.setClusterData(clusterData)
                log.info('setClusterData {}'.format(Decimal(time.time() - partstarttime).quantize(Decimal('.00001'))))

                partstarttime = time.time()
                await self.setClusterMaster(clusterMaster)
                log.info('setClusterMaster {}'.format(Decimal(time.time() - partstarttime).quantize(Decimal('.00001'))))

            except Exception as e:
                print(traceback.print_exc())

        await self.setClusterStatus(startBlock + blockCount - 1)
        #log.error('set cluster status - end {}'.format(Decimal(time.time() - starttime).quantize(Decimal('.00001'))))

        processingtime = Decimal(time.time() - starttime).quantize(Decimal('.00001'))
        log.info('[CLUSTERING {}] processing time {}'.format(self.coin, processingtime))
        log.info('==================')
        print('[CLUSTERING {}] processing time {}'.format(self.coin, processingtime))


    def mergeClusterProcessingData(self, newClusterData):
        # newClusterData = [{'cluster': {cid: [address_list]}, 'merge': {cid: [cid_list]}}]

        if not isinstance(newClusterData, list):
            newClusterData = [newClusterData]

        cidlist = []
        for data in newClusterData:
            cidlist.extend(list(data['cluster'].keys()))
            cidlist.extend(list(data['merge'].keys()))
        cidlist = sorted(set(cidlist))

        returnData = {'cluster': {}, 'merge': {}, 'new': []}

        # newClusterData 의 cluster, merge 데이터를 returnData 에 조건 없이 병합
        for cid in cidlist:
            for data in newClusterData:
                if cid in data['cluster']:
                    try:
                        returnData['cluster'][cid].extend(data['cluster'][cid])
                    except KeyError:
                        returnData['cluster'][cid] = []
                        returnData['cluster'][cid].extend(data['cluster'][cid])

                if cid in data['merge']:
                    try:
                        returnData['merge'][cid].extend(data['merge'][cid])
                    except KeyError:
                        returnData['merge'][cid] = []
                        returnData['merge'][cid].extend(data['merge'][cid])

            try:
                returnData['cluster'][cid] = sorted(set(returnData['cluster'][cid]))
                returnData['merge'][cid] = sorted(set(returnData['merge'][cid]))
            except KeyError:
                pass

        # returnData 의 cluster 데이터를 탐색해 주소 기준 병합
        for baseCid in cidlist:
            if baseCid not in returnData['cluster']:
                continue
            baseAddress = returnData['cluster'][baseCid]

            for targetCid in cidlist:
                if targetCid == baseCid or targetCid not in returnData['cluster']:
                    continue
                targetAddress = returnData['cluster'].pop(targetCid)

                if set(baseAddress).intersection(targetAddress):
                    returnData['cluster'][baseCid].extend(targetAddress)
                else:
                    returnData['cluster'][targetCid] = targetAddress

            returnData['cluster'][baseCid] = sorted(set(returnData['cluster'][baseCid]))

        # returnData 의 merge 데이터를 탐색해 cid 기준 병합
        for baseCid in cidlist:
            if baseCid not in returnData['merge']:
                continue
            baseAddress = returnData['merge'][baseCid]

            # 각 value 간의 교집합이 있으면 병합
            for targetCid in cidlist:
                if targetCid == baseCid or targetCid not in returnData['merge']:
                    continue
                targetAddress = returnData['merge'].pop(targetCid)

                if set(baseAddress).intersection(targetAddress):
                    returnData['merge'][baseCid].append(targetCid)
                    returnData['merge'][baseCid].extend(targetAddress)
                else:
                    returnData['merge'][targetCid] = targetAddress

            # baseCid 의 value 가 merge 데이터의 key 로도 존재한다면, 해당 merge 데이터의 key 의 value 를 baseCid 에 병합
            for targetCid in baseAddress:
                if targetCid != baseCid and targetCid in returnData['merge']:
                    returnData['merge'][baseCid].extend(returnData['merge'].pop(targetCid))

            try:
                returnData['merge'][baseCid] = sorted(set(returnData['merge'][baseCid]))
            except KeyError:
                pass

        # new 데이터를 병합
        for baseSeq in range(len(newClusterData)):
            if not newClusterData[baseSeq]['new']:
                continue
            baseAddress = newClusterData[baseSeq]['new'][1]
            # (newMaster, addressList, addressData[newMaster][1], addressData[newMaster][2])

            for targetSeq in range(len(newClusterData)):
                if baseSeq == targetSeq or not newClusterData[targetSeq]['new']:
                    continue
                targetAddress = newClusterData[targetSeq]['new'][1]

                if set(baseAddress).intersection(targetAddress):
                    master = newClusterData[baseSeq]['new'][0]
                    if newClusterData[baseSeq]['new'][2] > newClusterData[targetSeq]['new'][2]:
                        master = newClusterData[targetSeq]['new'][0]

                    addressList = sorted(set(newClusterData[baseSeq]['new'][1] + newClusterData[targetSeq]['new'][1]))
                    first_timestamp = min(newClusterData[baseSeq]['new'][2], newClusterData[targetSeq]['new'][2])
                    last_timestamp = max(newClusterData[baseSeq]['new'][2], newClusterData[targetSeq]['new'][2])

                    returnData['new'].append((master, addressList, first_timestamp, last_timestamp))

                    newClusterData[targetSeq]['new'] = None

        return returnData

    def calcClusterData(self, addressData):
        # addressData = {address: (cid, first_timestamp, last_timestamp), ...}
        # clusterData = {cid: [address1, address2, ...], ...}
        clusterIds = sorted(set([ addressData[address][0] for address in addressData if addressData[address][0] != None]))
        clusterData = dict.fromkeys(clusterIds)

        for address in addressData:
            cid = addressData[address][0]
            if cid:
                if clusterData[cid] == None:
                    clusterData[cid] = []
                clusterData[cid].append(address)

        return clusterData

    def updateAddressData(self, addressData, clusterData):
        # addressData = {address: (first_timestamp, cid), ...}
        # clusterData = {cid: [address1, address2, ...], ...}

        for address in list(addressData.keys()):
            clusterId = 0
            for cid in clusterData:
                if address in clusterData[cid]:
                    clusterId = cid
                    break

            first_txtime = addressData[address][1]
            last_txtime = addressData[address][2]
            addressData[address] = (clusterId, first_txtime, last_txtime)

        return addressData

    def updateClusterMaster(self, clusterMaster, addressData):
        # clusterMaster = {cid: (first_timestamp, last_timestamp, master), ...}
        # addressData = {address: (first_timestamp, cid), ...}

        for cid in list(clusterMaster.keys()):
            first_timestamp = 0
            last_timestamp = 0
            master = None

            for address in addressData:
                if addressData[address][0] == cid:
                    if first_timestamp == 0 or first_timestamp > addressData[address][1]:
                        first_timestamp = addressData[address][1]
                        master = address
                    if last_timestamp == 0 or last_timestamp < addressData[address][2]:
                        last_timestamp = addressData[address][2]

            if master:
                clusterMaster[cid] = (first_timestamp, last_timestamp, master)
            else:
                clusterMaster.pop(cid)

        return clusterMaster

    @classmethod
    def commonSpending(cls, coin, txid, addressData, clusterData):
        returnData = {'cluster': {}, 'merge': {}, 'new': None}
        addressList = list(addressData.keys())
        if '' in addressList:
            addressList.remove('')

        if not set([addressType(coin, address) for address in addressList]).intersection([None, ADDRTYPE_CONTRACT]):
            clusterId = []
            for cid in clusterData:
                if set(addressList).intersection(clusterData[cid]):
                    clusterId.append(cid)
            clusterId.sort()

            if len(clusterId) >= 1:
                newAddressList = addressList
                for cid in clusterId:
                    newAddressList.extend(clusterData[cid])

                returnData['cluster'][clusterId[0]] = newAddressList
                # merge 할때 중복 제거 하니까 굳이 리소스 낭비 하지 말것.

                if len(clusterId) > 1:
                    if clusterId[0] not in returnData['merge']:
                        returnData['merge'][clusterId[0]] = []
                    returnData['merge'][clusterId[0]].extend(clusterId[1:])

            else:
                newMaster = addressList[0]
                returnData['new'] = (newMaster, addressList, addressData[newMaster][1], addressData[newMaster][2])

        else:
            log.error('address type is None or Contract Type')
            log.error(addressList)

        return returnData

def addressType(coin, address):
    length = len(address)

    if length > 0:
        if coin == 'QTUM':
            if address[0] == 'Q' and (26 <= length and length <= 34):
                return ADDRTYPE_LEGACY
            elif address[0] == 'M' and (26 <= length and length <= 34):
                return ADDRTYPE_SEGWIT
            elif address[:3] == 'qc1' and (61 == length and length == 42):
                return ADDRTYPE_BECH32
            elif len(address) == 40:
                return ADDRTYPE_CONTRACT
            else:
                return None
        elif coin == 'BTC':
            # https://en.bitcoin.it/wiki/Address
            # https://en.bitcoin.it/wiki/List_of_address_prefixes
            if address[0] == '1' and (26 <= length and length <= 34):
                # P2PKH
                return ADDRTYPE_LEGACY
            elif address[0] == '3' and (26 <= length and length <= 34):
                # P2SH
                return ADDRTYPE_SEGWIT
            elif address[:3] == 'bc1' and (62 == length or length == 42):
                return ADDRTYPE_BECH32
            elif address[:2] == 'm_' and length == 34:
                return ADDRTYPE_MULTISIG
            elif address[:2] == 'u_' and length == 34:
                return ADDRTYPE_UNKNOWN
            elif length == 40:
                return ADDRTYPE_PUBKEYHASH
            else:
                return None
        else:
            print('coin({}) type is not defined'.format(coin))
            raise ValueError
    else:
        print('unexpected address({})'.format(address))
        return None

