import os
import asyncio
import traceback
import json
import time
import operator
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from http_client import RPCClient
from logger import log
import store

from objsize import get_deep_size

class Fetcher(RPCClient):
    def __init__(self, coin):
        super().__init__()

        self.coin = coin
        self.sleepSec = 10
        self.lastBlockNumber = 0 # genesis block(0) 에는 트랜잭션 데이터가 없음.
        self.prepareChunkCount = 3
        self.processBlockCount = 50
        self.processBLockPartialCount = int(self.processBlockCount / 5) # 1/5 가 적당함

        store.data['buffer'][coin] = {'fetching': {}, 'done': {}}
        self.insertBuffer = store.data['buffer'][self.coin]


    def getAPIServer(self, workerSequence):
        return self.apiurl[workerSequence % len(self.apiurl)]


    async def Terminate(self):
        store.data['running'] = False
        await self.closeSession()


    async def Process(self):
        while store.data['running']:
            while self.coin not in store.data['lastStatus']:
                await asyncio.sleep(1)
            lastStatus = store.data['lastStatus'][self.coin]

            while 'isRollbacking' not in store.data or True == store.data['isRollbacking']:
                await asyncio.sleep(0)

            try:
                tasks = []
                # 200721 블록 분기를 대비해 최종 블럭에서 -5까지만 따라간다, 개선 필요
                currentBlockNumber = await self.getLastBlockNumber() - 5
                if 0 == self.lastBlockNumber and 0 < lastStatus['block']:
                    self.lastBlockNumber = lastStatus['block']
                    print('get lastblocknumber from lastStatus', lastStatus['block'])

                if self.lastBlockNumber != currentBlockNumber:
                    blockCount = self.processBlockCount
                    blockPartialCount = self.processBLockPartialCount

                    for startBlockNumber in range(self.lastBlockNumber + 1, currentBlockNumber, blockCount):
                        endBlockNumber = min(startBlockNumber + blockCount - 1, currentBlockNumber)
                        if startBlockNumber in self.insertBuffer['done']:
                            print('already have item', startBlockNumber)
                            continue

                        if False and self.coin == 'BTC' and (endBlockNumber > 478558 or endBlockNumber > 491407):
                            print('BCH/BTG forking')
                            workingPath = os.path.abspath(os.path.dirname(__file__))
                            termFilename = '{}/force_terminate'.format(workingPath)
                            with open(termFilename, 'wt') as term:
                                term.write('stop')
                            store.data['running'] = False
                            break
                        
                        # async works making
                        works = []
                        for blockNumber in range(startBlockNumber, endBlockNumber, blockPartialCount):
                            currentBlockPartialCount = blockPartialCount
                            if blockNumber + currentBlockPartialCount > currentBlockNumber:
                                currentBlockPartialCount = currentBlockNumber - blockNumber + 1
                            works.append((startBlockNumber, blockNumber, currentBlockPartialCount))

                        for i in range(len(works)):
                            tasks.append(asyncio.ensure_future(self.Fetching(i, works[i][0], works[i][1], works[i][2])))

                        starttime = time.time()
                        print('processing: {} to {} / {}'.format(startBlockNumber, endBlockNumber, currentBlockNumber))

                        # async working
                        self.insertBuffer['fetching'][startBlockNumber] = []
                        await asyncio.gather(*tasks)
                        self.insertBuffer['fetching'][startBlockNumber].sort(key = operator.itemgetter('blocknum'))
                        self.lastBlockNumber = endBlockNumber

                        # for debug 200419
                        txdatas = self.insertBuffer['fetching'][startBlockNumber]
                        log.info('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
                        log.info('BLOCKNUM : {} to {}'.format(txdatas[0]['blocknum'], txdatas[-1]['blocknum']))
                        log.info('FIRST TXID : {}'.format(txdatas[0]['txid']))
                        log.info('LAST TXID : {}'.format(txdatas[-1]['txid']))
                        log.info('TX LENGTH : {}'.format(len(txdatas)))
                        log.info('DATETIME : {}'.format(datetime.fromtimestamp(txdatas[0]['timestamp'])))
                        log.info('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')

                        # move from fetching buffer to done buffer
                        processingBlockCount = endBlockNumber - startBlockNumber + 1
                        fetchingData = self.insertBuffer['fetching'].pop(startBlockNumber)
                        self.insertBuffer['done'][startBlockNumber] = {}
                        self.insertBuffer['done'][startBlockNumber]['data'] = fetchingData
                        self.insertBuffer['done'][startBlockNumber]['transCount'] = len(fetchingData)
                        self.insertBuffer['done'][startBlockNumber]['blockCount'] = processingBlockCount

                        # processing time calculating
                        processingtime = Decimal(time.time() - starttime).quantize(Decimal('.001'))
                        timeperblock = Decimal(processingtime / processingBlockCount).quantize(Decimal('.001'))
                        sleeptime = 0
                        if processingtime > 1:
                            sleeptime = min(30, int(processingtime / 3) + 1)
                        print('processing time {} ({}/block)'.format(processingtime, timeperblock))
                        print('sleep {} sec'.format(sleeptime))

                        # insertBuffer limit checking
                        size = int(get_deep_size(self.insertBuffer['done']) / 1024 / 1024)
                        print('length of insertBuffer fetching: {} done: {} ({} MB)'.format(len(self.insertBuffer['fetching']), len(self.insertBuffer['done']), size))
                        while len(self.insertBuffer['done']) >= self.prepareChunkCount:
                            #print('too many buffer size {}, sleep more {} sec'.format(len(self.insertBuffer['done']), sleeptime))
                            await asyncio.sleep(sleeptime)

                        await asyncio.sleep(sleeptime)
                else:
                    print('no new blocks. sleep {} secs'.format(self.sleepSec))
                    await asyncio.sleep(self.sleepSec)

            except Exception as e:
                print(e)
                print(traceback.format_exc())
                store.data['running'] = False

    async def Fetching(self, workerSequence, startBlockNumber = 0, blockNumber = 0, requestCount = 1):
        try:
            txIds = await self.getTransactionIds(workerSequence, blockNumber, requestCount)
            #print(startBlockNumber, blockNumber, requestCount, txIds[0])
            txDatas = []
            if len(txIds) > 0:
                txDatas = await self.getTransactionData(workerSequence, txIds)
                self.insertBuffer['fetching'][startBlockNumber].extend(txDatas)
                #self.lastBlockNumber = max([txData['blocknum'] for txData in txDatas] + [self.lastBlockNumber])
            #print(max([txData['blocknum'] for txData in txDatas]))
        except Exception as e:
            print(e)
            print(traceback.format_exc())
