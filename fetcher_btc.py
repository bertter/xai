import asyncio
import time
import json
import hashlib, binascii, base58
from decimal import Decimal
from fetcher_base import Fetcher
from utils import dropTailingZeros
from logger import log
import traceback

from objsize import get_deep_size

class BTCFetcher(Fetcher):
    def __init__(self, coin):
        super().__init__(coin)
        self.apiurl = ['http://bugzero:bugzero@localhost:8332', 'http://bugzero:bugzero@localhost:8342', 'http://bugzero:bugzero@localhost:8352', 'http://bugzero:bugzero@localhost:8362']

        self.printDebugProcTime = False

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

        responseData = {}
        for i in range(len(blockDatas)):
            blockData = blockDatas[i]
            for txid in blockData['result']['tx']:
                responseData[txid] = {'timestamp': blockData['result']['time'], 'blocknum': blockHashs[i]['id']}

        return responseData

    async def getRawTransactionData(self, workerSequence, txIds):
        ##
        # GET RAW TRANSACTION DATA
        ##
        starttime = time.time()
        requestData = []
        rawTxDatas = {}
        txIdList = list(txIds.keys())
        for i in range(len(txIdList)):
            requestData.append({'method': 'getrawtransaction', 'params': [txIdList[i]], 'id': i})

            # https://github.com/aio-libs/aiohttp/blob/master/aiohttp/payload.py
            # TOO_LARGE_BYTES_BODY = 2 ** 20  # 1 MB
            if 500 * 1000 < int(get_deep_size(requestData)):
                response = await self.requestBatchPost(self.getAPIServer(workerSequence), data = requestData[:-1])
                for resp in response:
                    rawTxDatas[resp['id']] = resp['result']
                requestData = [requestData[-1]]

        response = await self.requestBatchPost(self.getAPIServer(workerSequence), data = requestData)
        for resp in response:
            rawTxDatas[resp['id']] = resp['result']

        processingtime = Decimal(time.time() - starttime).quantize(Decimal('.001'))
        if self.printDebugProcTime:
            log.info('getrawtransaction {} - {}'.format(workerSequence, processingtime))

        
        ##
        # DECODE RAW TRANSACTION DATA
        ##
        starttime = time.time()
        requestData = []
        txDatas = {}
        witnessTxJsonIds = []
        for jsonId in rawTxDatas.keys():
            rawTxData = rawTxDatas[jsonId]
            requestData.append({'method': 'decoderawtransaction', 'params': [rawTxData], 'id': jsonId})
            #log.info('add request data: {} - {} - {}'.format(workerSequence, txIdList[i], int(get_deep_size(rawTxData))))

            if 500 * 1000 < int(get_deep_size(requestData)):
                #log.info('request_size: {} - {}'.format(workerSequence, int(get_deep_size(requestData[:-1]))))
                response = await self.requestBatchPost(self.getAPIServer(workerSequence), data = requestData[:-1])
                for resp in response:
                    if len(resp['result']['vin']) == 0:
                        witnessTxJsonIds.append(resp['id'])
                    else:
                        txDatas[resp['id']] = resp['result']
                requestData = [requestData[-1]]

        #log.info('request_size: {} - {}'.format(workerSequence, int(get_deep_size(requestData))))
        response = await self.requestBatchPost(self.getAPIServer(workerSequence), data = requestData)
        for resp in response:
            if len(resp['result']['vin']) == 0:
                witnessTxJsonIds.append(resp['id'])
            else:
                txDatas[resp['id']] = resp['result']

        processingtime = Decimal(time.time() - starttime).quantize(Decimal('.001'))
        if self.printDebugProcTime:
            log.info('decoderawtransaction {} - {}'.format(workerSequence, processingtime))


        ##
        # GET WITNESS TRANSACTION DATA
        ##
        if len(witnessTxJsonIds) > 0:
            starttime = time.time()
            requestData = []
            for jsonId in witnessTxJsonIds:
                # if given 2nd param 'True' then returning data is json format (like after decode raw transaction)
                requestData.append({'method': 'getrawtransaction', 'params': [txIdList[jsonId], True], 'id': jsonId})

                # https://github.com/aio-libs/aiohttp/blob/master/aiohttp/payload.py
                # TOO_LARGE_BYTES_BODY = 2 ** 20  # 1 MB
                if 500 * 1000 < int(get_deep_size(requestData)):
                    response = await self.requestBatchPost(self.getAPIServer(workerSequence), data = requestData[:-1])
                    for resp in response:
                        txDatas[resp['id']] = resp['result']
                    requestData = [requestData[-1]]

            response = await self.requestBatchPost(self.getAPIServer(workerSequence), data = requestData)
            for resp in response:
                txDatas[resp['id']] = resp['result']

            processingtime = Decimal(time.time() - starttime).quantize(Decimal('.001'))
            if self.printDebugProcTime:
                log.info('getrawtransaction(witness) {} - {}'.format(workerSequence, processingtime))


        return [ txDatas[key] for key in sorted(txDatas) ]


    async def getReceiverData(self, workerSequence, txIds):
        txDatas = await self.getRawTransactionData(workerSequence, txIds)

        responseData = {}
        for i in range(len(txDatas)):
            txData = txDatas[i]
            txId = txData['txid']

            for sequence in range(len(txData['vout'])):
                vout = txData['vout'][sequence]

                amount = Decimal(str(vout['value']))
                receiver = self.getAddress(workerSequence, txId, vout['scriptPubKey'])
                if not receiver:
                    receiver = self.getHashAddress(prefix='u_', data='{}_{}'.format(txId, sequence))

                if txId not in responseData:
                    responseData[txId] = []
                responseData[txId].append((receiver, amount))

        return responseData


    async def getTransactionData(self, workerSequence, txIds):
        txDatas = await self.getRawTransactionData(workerSequence, txIds)

        receiverTxid = {}
        for txData in txDatas:
            for vin in txData['vin']:
                if 'coinbase' not in vin:
                    receiverTxid[vin['txid']] = {}
        receiverDatas = await self.getReceiverData(workerSequence, receiverTxid)

        starttime = time.time()
        responseData = []
        for i in range(len(txDatas)):
            txData = txDatas[i]
            txId = txData['txid']

            try:
                parseData = {'txid': txId,
                             'timestamp': txIds[txId]['timestamp'],
                             'blocknum': txIds[txId]['blocknum'],
                             'sender_amount': [],
                             'sender': [],
                             'receiver': [],
                             'receiver_amount': [],
                             'total_amount': Decimal(str(0))}
            except KeyError:
                import pprint
                print('txid: {}'.format(txId))
                print('txDatas[{}]'.format(i))
                pprint.pprint(txDatas[i])
                print('-'*20)
                print('txid in txIds: {}'.format(txId in txIds))
            except Exception as e:
                print(traceback.format_exc())
                print(i)
                print(len(txDatas))
                print(len(txIds))
                #print(txDatas[i])
                #print(txIds[i])


            for data in txData['vin']:
                sender = ''
                sender_amount = 0
                if 'coinbase' in data:
                    sender = 'Mining'
                else:
                    try:
                        (sender, sender_amount) = receiverDatas[data['txid']][data['vout']]
                    except Exception as e:
                        print(txIds[i][0])
                        print(json.dumps(txData))
                        print('------')
                        print(data['txid'])
                        print('------')
                        print(e)
                        print(traceback.format_exc())

                if 'sender' not in parseData:
                    parseData['sender'] = [sender]
                else:
                    parseData['sender'].append(sender)

                if 'sender_amount' not in parseData:
                    parseData['sender_amount'] = [sender_amount]
                else:
                    parseData['sender_amount'].append(Decimal(str(sender_amount)))

            for sequence in range(len(txData['vout'])):
                data = txData['vout'][sequence]
                receiver = None
                receiver_amount = data['value']
                parseData['total_amount'] += Decimal(str(receiver_amount))

                try:
                    receiver = self.getAddress(workerSequence, txId, data['scriptPubKey'])
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
                    receiver = self.getHashAddress(prefix='u_', data='{}_{}'.format(txId, sequence))
                    '''
                    log.info('Unknown scriptPubkey')
                    log.info('txid: {}'.format(txId))
                    log.info('scriptPubkey: {}'.format(data['scriptPubKey']))
                    log.info('make hash address: {}'.format(receiver))
                    '''

                if 'receiver' not in parseData:
                    parseData['receiver'] = [receiver]
                else:
                    parseData['receiver'].append(receiver)

                if 'receiver_amount' not in parseData:
                    parseData['receiver_amount'] = [receiver_amount]
                else:
                    parseData['receiver_amount'].append(Decimal(str(receiver_amount)))

            responseData.append(parseData)

        processingtime = Decimal(time.time() - starttime).quantize(Decimal('.001'))

        if self.printDebugProcTime:
            log.info('calc txdatas {} - {}'.format(workerSequence, processingtime))

        #print(json.dumps(txData))
        #print('----')
        #input(responseData)

        return responseData

    def getAddress(self, workerSequence, txid, scriptPubKey):
        address = None
        sendType = scriptPubKey['type']

        if 'call' == sendType:
            address = scriptPubKey['asm'].split(' ')[4]
        elif sendType in ['pubkeyhash', 'scripthash', 'witness_v0_scripthash', 'witness_v0_keyhash']:
            address = scriptPubKey['addresses'][0]
        elif 'pubkey' == sendType:
            if 'addresses' in scriptPubKey:
                address = scriptPubKey['addresses'][0]
            else:
                p2pk = scriptPubKey['asm'].split(' ')[0]
                ripemd160 = hashlib.new('ripemd160')
                ripemd160.update(binascii.unhexlify(hashlib.sha256(binascii.unhexlify(p2pk.encode())).hexdigest().encode()))
                address = ripemd160.hexdigest()
                address = base58.b58encode_check(binascii.unhexlify(('00'+address).encode())).decode()
        elif 'multisig' == sendType:
            address = ','.join(scriptPubKey['addresses'])
            address = self.getHashAddress(prefix='m_', data=address)
        elif 'nonstandard' == sendType:
            address = None
            if 'addresses' in scriptPubKey:
                address = scriptPubKey['addresses'][0]
            else:
                try:
                    address = [ asm for asm in scriptPubKey['asm'].split(' ') if asm[:3] != 'OP_' ][0]
                    address = self.getHashAddress(prefix='u_', data=address)
                except IndexError:
                    address = None
        elif 'nulldata' == sendType:
            address = None
        else:
            log.info('Unknown sendtype of address: {}'.format(sendType))
            log.info('txid: {}'.format(txid))
            log.info('scriptPubkey: {}'.format(scriptPubKey))
            address = None

        return address

    def getHashAddress(self, prefix, data):
        enc = hashlib.md5()
        enc.update(data.encode('ascii'))
        address = prefix + enc.hexdigest()
        return address
