from fetcher_base import Fetcher

import asyncio
import time
import json
from decimal import Decimal
from utils import dropTailingZeros, DecimalEncode
from logger import log
import traceback

class ETHFetcher(Fetcher):
    def __init__(self, coin):
        super().__init__(coin)
        self.apiurl = ['http://127.0.0.1:5458']
        self.wei = 1000000000000000000

    def validCheck(self, data, plainText):
        errorIds = []
        if not plainText:
            checkDatas = json.loads(data)
            if not isinstance(checkDatas, list):
                checkDatas = [checkDatas]
            for checkData in checkDatas:
                if 'error' in checkData or None == checkData['result']:
                    errorIds.append(checkData['id'])
                    log.error('checkdata error')
                    log.error(checkData)

        if len(errorIds):
            return errorIds
        return True

    async def getLastBlockNumber(self, workerSequence = 0):
        data = await self.requestPost(self.getAPIServer(workerSequence), data = {
            'id': 0,
            'method': 'eth_blockNumber',
            'params': [],
        })
        return int(data['result'], 16)

    async def getBalance(self, workerSequence, address):
        data = await self.requestPost(self.getAPIServer(workerSequence), data = {
            'id': 0,
            'method': 'eth_getBalance',
            'params': [address, 'latest'],
        })
        return Decimal(dropTailingZeros(str(Decimal(data['result'], 16) / Decimal(self.wei))))

    async def getTransactionCount(self, workerSequence, address):
        data = await self.requestPost(self.getAPIServer(workerSequence), data = {
            'id': 0,
            'method': 'eth_getTransactionCount',
            'params': [address, 'latest'],
        })
        return int(data['result'], 16)

    async def getTransactionReceipt(self, workerSequence, txid):
        data = await self.requestPost(self.getAPIServer(workerSequence), data = {
            'id': 0,
            'method': 'eth_getTransactionReceipt',
            'params': [txid],
        })
        return data['result']

    async def getTransactionIds(self, workerSequence, blockStart, requestCount):
        requestData = []
        for blocknum in range(blockStart, blockStart + requestCount):
            requestData.append({'method': 'eth_getBlockByNumber', 'params': [hex(blocknum), True], 'id': blocknum})
        blockDatas = await self.requestBatchPost(self.getAPIServer(workerSequence), data = requestData)

        responseData = []
        for i in range(len(blockDatas)):
            blockData = blockDatas[i]
            for txdata in blockData['result']['transactions']:
                # (txid, timestamp, blocknum)
                responseData.append((txdata['hash'], int(blockData['result']['timestamp'], 16), blockData['id']))

        blockcount = {}
        for txdata in responseData:
            if txdata[2] not in blockcount:
                blockcount[txdata[2]] = 1
            else:
                blockcount[txdata[2]] += 1
        log.info('blockCount: {}'.format(blockcount))

        return responseData

    async def getTransactionData(self, workerSequence, txIds):
        packetBytes = 0
        requestData = []
        txDatas = []

        blockcount = {}

        for i in range(len(txIds)):
            data = {'method': 'eth_getTransactionByHash', 'params': [txIds[i][0]], 'id': i}
            packetBytes += len(str(data))

            if packetBytes >= 1024 * 500:
                txData = await self.requestBatchPost(self.getAPIServer(workerSequence), data = requestData)
                txDatas.extend(txData)
                packetBytes = 0
                requestData = []

            requestData.append(data)

            if txIds[i][2] not in blockcount:
                blockcount[txIds[i][2]] = 1
            else:
                blockcount[txIds[i][2]] += 1
        log.info('blockCount: {} / {}'.format(blockcount, sum(blockcount.values())))


        if len(requestData) > 0:
            txData = await self.requestBatchPost(self.getAPIServer(workerSequence), data = requestData)
            txDatas.extend(txData)

        log.info('txdataCount: {}'.format(len(txDatas)))


        responseData = []
        for i in range(len(txDatas)):
            txData = txDatas[i]['result']
            parseData = {}
            try:
                amount = Decimal(int(txData['value'], 16)) / Decimal(self.wei)
                memo = {'gas': Decimal(int(txData['gas'], 16)), 'gasPrice': Decimal(int(txData['gasPrice'], 16))}
                parseData = {'txid': txIds[i][0],
                             'timestamp': txIds[i][1],
                             'blocknum': txIds[i][2],
                             'sender_amount': [amount],
                             'sender': [txData['from']],
                             'receiver': [txData['to']],
                             'receiver_amount': [amount],
                             'total_amount': amount,
                             'memo': str(json.dumps(memo, default = DecimalEncode))}
                if not parseData['receiver'][0]:
                    txReceipt = await self.getTransactionReceipt(workerSequence, txIds[i][0])
                    parseData['receiver'][0] = txReceipt['contractAddress']


            except Exception as e:
                print(e)
                print(traceback.format_exc())
                print(i)
                print(txData)
                print(len(txDatas))
                print(len(txIds))
                #print(txDatas[i])
                #print(txIds[i])

            responseData.append(parseData)

        #print(json.dumps(txData))
        #print('----')
        #input(responseData)

        return responseData
