from fetcher_qtum import QTUMFetcher
from fetcher_eth import ETHFetcher
from fetcher_btc import BTCFetcher
from cluster import Cluster

dbConfig = {
	'host': '127.0.0.1',
	'port': 3306,
	'user': 'cctracker',
	'passwd': 'trackerPass12#',
}

cluster = {
	#'BTC': Cluster,
}

fetcher = {
	'QTUM': QTUMFetcher,
	#'BTC': BTCFetcher,
	#'ETH': ETHFetcher,
}
