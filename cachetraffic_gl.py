
from gevent.queue import Queue
from gevent.lock import BoundedSemaphore

global tasks_dayqueue
# Queue processing log files
tasks_dayqueue = Queue()
global tasks_daydict
tasks_daydict = {}
global tasks_daycheckdict
tasks_daycheckdict = {}
global tasks_daysem
tasks_daysem = BoundedSemaphore(1)

global tasks_repqueue
tasks_repqueue = Queue()

global sqlconf_dict
sqlconf_dict = {}
sqlextensive_dict = {}
global sqlconf_sem
sqlconf_sem = BoundedSemaphore(1)

global sqlconpool_dict
sqlconpool_dict = {}
global sqlconpool_sem
sqlconpool_sem = BoundedSemaphore(1)

global tasks_historyqueue
tasks_historyqueue = Queue()
