import asyncio
import time
import json
import hashlib, binascii
from decimal import Decimal
from fetcher_base import Fetcher
from utils import dropTailingZeros
from logger import log
import traceback

class QTUMFetcher(Fetcher):
    def __init__(self, coin):
        super().__init__(coin)
        self.apiurl = ['http://bugzero:bugzero@localhost:3889']

    def validCheck(self, data, plainText):
        errorIds = []
        if not plainText:
            checkDatas = json.loads(data)
            if not isinstance(checkDatas, list):
                checkDatas = [checkDatas]
            for checkData in checkDatas:
                if None != checkData['error']:
                    errorIds.append(checkData['id'])

        if len(errorIds):
            return errorIds
        return True

    async def getLastBlockNumber(self, workerSequence = 0):
        data = await self.requestPost(self.getAPIServer(workerSequence), data = {
            'method': 'getblockcount',
            'params': [],
        })
        return data['result']

    async def getTransactionIds(self, workerSequence, blockStart, requestCount):
        requestData = []
        for blocknum in range(blockStart, blockStart + requestCount):
            requestData.append({'method': 'getblockhash', 'params': [blocknum], 'id': blocknum})
        blockHashs = await self.requestBatchPost(self.getAPIServer(workerSequence), data = requestData)

        requestData = []
        for blockHash in blockHashs:
            requestData.append({'method': 'getblock', 'params': [blockHash['result']], 'id': blockHash['id']})
        blockDatas = await self.requestBatchPost(self.getAPIServer(workerSequence), data = requestData)

        responseData = []
        for i in range(len(blockDatas)):
            blockData = blockDatas[i]
            for txid in blockData['result']['tx']:
                responseData.append((txid, blockData['result']['time'], blockHashs[i]['id']))

        return responseData

    async def getReceiverData(self, workerSequence, txId, sequence = 0):
        rawTxData = await self.requestPost(self.getAPIServer(workerSequence), data = {
            'method': 'getrawtransaction',
            'params': [txId],
        })
        txData = await self.requestPost(self.getAPIServer(workerSequence), data = {
            'method': 'decoderawtransaction',
            'params': [rawTxData['result']],
        })

        data = txData['result']['vout'][sequence]
        amount = Decimal(str(data['value']))
        receiver = await self.getAddress(workerSequence, txId, data['scriptPubKey'])

        return receiver, amount

    async def getTransactionData(self, workerSequence, txIds):
        requestData = []
        for i in range(len(txIds)):
            requestData.append({'method': 'getrawtransaction', 'params': [txIds[i][0]], 'id': i})
        rawTxDatas = await self.requestBatchPost(self.getAPIServer(workerSequence), data = requestData)

        '''
        requestData = []
        for rawTxData in [ (rawTxData['result'], rawTxData['id']) for rawTxData in rawTxDatas ]:
            requestData.append({'method': 'decoderawtransaction', 'params': [rawTxData[0]], 'id': rawTxData[1]})
        txDatas = await self.requestBatchPost(self.getAPIServer(workerSequence), data = requestData)
        '''

        txDatas = []
        while 0 < len(rawTxDatas):
            paramsTotal = 0
            requestData = []
            for i in range(len(rawTxDatas)):
                rawTxData = rawTxDatas[i]['result']
                jsonId = rawTxDatas[i]['id']
                paramsTotal += len(rawTxData)

                if 500000 < paramsTotal:
                    rawTxDatas = rawTxDatas[i:]
                    break
                else:
                    requestData.append({'method': 'decoderawtransaction', 'params': [rawTxData], 'id': jsonId})
            else:
                rawTxDatas = []

            response = await self.requestBatchPost(self.getAPIServer(workerSequence), data = requestData)
            txDatas.extend(response)

        responseData = []

        for i in range(len(txDatas)):
            txData = txDatas[i]['result']
            txId = txIds[i][0]
            try:
                parseData = {'txid': txIds[i][0],
                             'timestamp': txIds[i][1],
                             'blocknum': txIds[i][2],
                             'sender_amount': [],
                             'sender': [],
                             'receiver': [],
                             'receiver_amount': [],
                             'total_amount': Decimal(str(0))}
            except Exception as e:
                print(e)
                print(traceback.format_exc())
                print(i)
                print(len(txDatas))
                print(len(txIds))
                #print(txDatas[i])
                #print(txIds[i])
                print(rawTxDatas)


            for data in txData['vin']:
                sender = ''
                sender_amount = 0
                if 'coinbase' in data:
                    sender = 'Mining'
                else:
                    prevTxid = data['txid']
                    prevSeq = data['vout']
                    try:
                        sender, sender_amount = await self.getReceiverData(workerSequence, prevTxid, prevSeq)
                    except Exception as e:
                        print('------')
                        print('error on getReceiverData')
                        print('txid: {}'.format(txIds[i][0]))
                        print('prev txid: {}'.format(prevTxid))
                        print('------')

                        log.info('------')
                        log.info('error on getReceiverData')
                        log.info('txid: {}'.format(txIds[i][0]))
                        log.info(json.dumps(txData))
                        log.info('------')
                        log.info('prev txid: {}'.format(prevTxid))
                        log.info('------')
                        log.info(e)
                        log.info(traceback.format_exc())

                if 'sender' not in parseData:
                    parseData['sender'] = [sender]
                else:
                    parseData['sender'].append(sender)

                if 'sender_amount' not in parseData:
                    parseData['sender_amount'] = [sender_amount]
                else:
                    parseData['sender_amount'].append(sender_amount)

            for data in txData['vout']:
                receiver = None
                receiver_amount = Decimal(str(data['value']))
                parseData['total_amount'] += receiver_amount

                try:
                    receiver = await self.getAddress(workerSequence, txId, data['scriptPubKey'])
                except Exception as e:
                    print('------')
                    print('error on getAddress')
                    print('txid: {}'.format(txId))
                    print('------')

                    log.info('------')
                    log.info('error on getAddress')
                    log.info('txid: {}'.format(txId))
                    log.info(data['scriptPubKey'])
                    log.info('------')
                    log.info(e)
                    log.info(traceback.format_exc())

                if not receiver:
                    continue

                # for debug 200303
                # clustering.py 144 line
                # if receiverData[address][1] == int(time.mktime(txtime.timetuple())):
                # KeyError: ''
                if receiver == '':
                    log.info('Blank Address')
                    log.info('txid: {}'.format(txId))
                    log.info('scriptPubkey: {}'.format(data['scriptPubKey']))
                    continue

                # debug 200302, contert from p2pk to p2pkh validation check
                '''
                if data['scriptPubKey']['type'] == 'pubkey':
                    print('txid: ', txId)
                    print('p2pk: ', data['scriptPubKey']['asm'].split(' ')[0])
                    print('p2pkh: ', receiver)
                '''

                if 'receiver' not in parseData:
                    parseData['receiver'] = [receiver]
                else:
                    parseData['receiver'].append(receiver)

                if 'receiver_amount' not in parseData:
                    parseData['receiver_amount'] = [receiver_amount]
                else:
                    parseData['receiver_amount'].append(receiver_amount)

            responseData.append(parseData)

        #print(json.dumps(txData))
        #print('----')
        #input(responseData)

        return responseData

    async def getAddress(self, workerSequence, txid, scriptPubKey):
        address = None
        sendType = scriptPubKey['type']

        if 'call' == sendType:
            address = scriptPubKey['asm'].split(' ')[4]
        elif 'pubkey' == sendType:
            if 'addresses' in scriptPubKey:
                address = scriptPubKey['addresses'][0]
            else:
                p2pk = scriptPubKey['asm'].split(' ')[0]
                ripemd160 = hashlib.new('ripemd160')
                ripemd160.update(binascii.unhexlify(hashlib.sha256(binascii.unhexlify(p2pk.encode())).hexdigest().encode()))
                address = ripemd160.hexdigest()
                # length of p2pk address is 40
               
        elif sendType in ['pubkeyhash', 'scripthash', 'witness_v0_scripthash', 'witness_v0_keyhash']:
            address = scriptPubKey['addresses'][0]
        elif 'nonstandard' == sendType:
            address = None
        elif 'nulldata' == sendType:
            address = None
        elif sendType in ['create', 'call_sender', 'create_sender']:
            txreceipt = await self.requestPost(self.getAPIServer(workerSequence), data = {
                'method': 'gettransactionreceipt',
                'params': [txid],
            })
            address = txreceipt['result'][0]['contractAddress']
        else:
            log.info('Unknown sendtype of address: {}'.format(sendType))
            log.info('txid: {}'.format(txid))
            log.info('scriptPubkey: {}'.format(scriptPubKey))
            address = None

        if address and len(address) == 40:
            decodedAddress = await self.requestPost(self.getAPIServer(workerSequence), data = {
                'method': 'fromhexaddress',
                'params': [address],
            })
            address = decodedAddress['result']

        return address

    def getHashAddress(self, prefix, data):
        enc = hashlib.md5()
        enc.update(data.encode('ascii'))
        address = prefix + enc.hexdigest()
        return address
