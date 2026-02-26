from concurrent import futures
import multiprocessing
import asyncio
from logger import log
from config import fetcher, cluster, dbConfig
import db_manager
import store
import time
import sys, os

WORKER_COUNT = 20
DB_WORKER_COUNT = int(WORKER_COUNT / 2)

def clusterBot(sharedData):
    store.init()
    store.data['running'] = True

    clusterBots = []
    for coin in cluster:
        clusterBots.append(cluster[coin](coin, dbConfig, DB_WORKER_COUNT))
        print('Cluster for {} was initialized'.format(coin))

    tasks = []
    tasks.extend([clusterBot.Process() for clusterBot in clusterBots])

    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.wait(tasks))
    except KeyboardInterrupt:
        print('Keyboard interrupt, wait closing')
        store.data['running'] = False

        tasks = []
        tasks.extend([clusterBot.Terminate() for clusterBot in clusterBots])
        loop.run_until_complete(asyncio.wait(tasks))

        pending = asyncio.Task.all_tasks()
        loop.run_until_complete(asyncio.gather(*pending))

def fetcherBot(sharedData):
    store.init()
    store.data['running'] = True

    # fetchbot, dbManager 초기화
    fetchBots = []
    dbManagers = []
    for coin in fetcher:
        store.data['buffer'][coin] = {}
        dbManager = db_manager.dbManager(coin, dbConfig, DB_WORKER_COUNT)
        dbManagers.append(dbManager)
        fetchBots.append(fetcher[coin](coin))
        print('Fetcher for {} was initialized'.format(coin))

    tasks = []
    tasks.extend([dbManager.Process() for dbManager in dbManagers])
    tasks.extend([fetchBot.Process() for fetchBot in fetchBots])

    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.wait(tasks))
    except KeyboardInterrupt:
        print('Keyboard interrupt, wait closing')
        store.data['running'] = False

        tasks = []
        tasks.extend([dbManager.Terminate() for dbManager in dbManagers])
        tasks.extend([fetchBot.Terminate() for fetchBot in fetchBots])
        loop.run_until_complete(asyncio.wait(tasks))

        pending = asyncio.Task.all_tasks()
        loop.run_until_complete(asyncio.gather(*pending))

def process(args):
    worker = args[0]
    sharedData = args[1]
    worker(sharedData)

def checkDatabase():
    loop = asyncio.get_event_loop()

    print(' - db for Fetcher')
    dbManagers = []
    for coin in fetcher:
        dbManager = db_manager.dbManager(coin, dbConfig, DB_WORKER_COUNT)
        dbManagers.append(dbManager)
    if len(dbManagers) > 1:
        tasks = [dbManager.checkTable() for dbManager in dbManagers]
        loop.run_until_complete(asyncio.wait(tasks))
    print(' - db for Fetcher: done') 

    print(' - db for Cluster')
    clusterBots = []
    for coin in cluster:
        clusterBots.append(cluster[coin](coin, dbConfig, DB_WORKER_COUNT))
    if len(clusterBots) > 1:
        tasks = [clusterBot.checkTable() for clusterBot in clusterBots]
        loop.run_until_complete(asyncio.wait(tasks))
    print(' - db for Cluster: done')

if __name__ == '__main__':
    workingPath = os.path.abspath(os.path.dirname(__file__))
    pidCheck = '{}/CCFetcher.pid'.format(workingPath)
    termCheck = '{}/force_terminate'.format(workingPath)

    if os.path.exists(pidCheck):
        print('CCFetcher is already running')
        log.error('CCFetcher is already running')
        sys.exit(-1)

    if os.path.exists(termCheck):
        os.remove(termCheck)

    with open(pidCheck, 'wt') as pid:
        pid.write(str(os.getpid()))

    print('Check Database')
    checkDatabase()

    manager = multiprocessing.Manager()
    sharedData = manager.dict()
    sharedData.update({'running': True})
    if len(sys.argv) > 1:
        sharedData.update({'coin': sys.argv[1:]})

    with futures.ProcessPoolExecutor(max_workers=WORKER_COUNT) as executor:
        future = executor.map(process, [(fetcherBot, sharedData), (clusterBot, sharedData)])
        (done, doing) = futures.wait(future, timeout=2)
