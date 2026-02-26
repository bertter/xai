import asyncio
from manager import tracker_manager
from db_manager import DBManager
from config import target, apikey
from tracker_config import tracker

from logger import log
from apikey_store import APIKeyStore
import store


store.init()

while True:
	# trackbot 초기화
	trackBots = []
	for coin, address, datasrc, delay, link, debug in target:
		if coin not in tracker:
			print('Tracker for %s has not exists' % coin)
		else:
			keystore = []
			if coin in apikey:
				keystore = APIKeyStore(coin, apikey[coin])
			else:
				print('APIKey for %s tracker has not exists' % coin)

			trackBot = tracker[coin](coin, address, datasrc, link, keystore, delay, debug)
			trackBots.append(trackBot)

			print('Tracker for %s...%s was initialized' % (address[:5], address[-5:]))

	'''
	if len(target):
		for trackBot in trackBots:
			coin = trackBot.getCoin()
			trackBot.setTransactionLimit(transactionLimits[coin])
			trackBot.setSlackWebHookURL(slackWebHookURL)
			trackBot.setDbManager(db_manager.DBManager(coin, dbConfig))
	'''

	tasks = [tracker_manager.Process()]
	tasks.extend([trackBot.Process() for trackBot in trackBots])

	try:
		store.data['running'] = True
		eventLoop = asyncio.get_event_loop()
		eventLoop.run_until_complete(asyncio.wait(tasks))
	except KeyboardInterrupt:
		print('Keyboard interrupt, wait closing')
		store.data['running'] = False

		if 0 < len(trackBots):
			tasks = []
			tasks.extend([trackBot.Terminate() for trackBot in trackBots])
			eventLoop.run_until_complete(asyncio.wait(tasks))

			pending = asyncio.Task.all_tasks()
			eventLoop.run_until_complete(asyncio.gather(*pending))
	finally:
		[]
		#eventLoop.close()

	print('restart Tracker')