import asyncio
import datetime
import store

class TrackerManager:
	def __init__(self):
		self.reset()

	def reset(self):
		self.lastseen = {}
		self.status = {}

	def now(self):
		return datetime.datetime.now().timestamp()

	def getStatusCount(self, status):
		return sum(value == status for value in self.status.values())

	def getStatus(self, target):
		return self.status[target]

	def setStatus(self, target, status):
		self.status[target] = status

	def getTotalCount(self):
		return len(self.status)

	def getAliveCount(self):
		return self.getStatusCount('processing') + self.getStatusCount('sleeping') + self.getStatusCount('delayed')

	def touch(self, target):
		self.lastseen[target] = self.now()
		self.setStatus(target, 'processing')

	def sleep(self, target):
		self.setStatus(target, 'sleeping')

	def delay(self, target):
		self.setStatus(target, 'delayed')

	def die(self, target):
		self.setStatus(target, 'dead')

	def suspect(self, target):
		self.setStatus(target, 'suspicious')


	def check(self):
		now = self.now()
		for target in [ t for t in self.status if self.status[t] == 'processing' ]:
			# x초 이상 대기시 delayed 로 판단
			if self.lastseen[target] + 5 <= now:
				self.delay(target)

	def print(self):
		if not store.data['running']:
			return

		status = 'Total: %d, Processing: %d (Delayed: %d), Sleeping: %d, Dead: %d, Suspicious: %d' % (self.getTotalCount(),
																									  self.getStatusCount('processing') + self.getStatusCount('delayed'),
																									  self.getStatusCount('delayed'),
																									  self.getStatusCount('sleeping'),
																									  self.getStatusCount('dead'),
																									  self.getStatusCount('suspicious'))

		print('-' * len(status))
		print(status)
		print('-' * len(status))

	async def Process(self):
		self.reset()
		while store.data['running']:
			await asyncio.sleep(60)
			self.check()
			self.print()
			if 0 < len(self.status) and 0 == self.getAliveCount():
				print('manager dead')
				break

tracker_manager = TrackerManager()