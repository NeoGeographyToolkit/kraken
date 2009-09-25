# asynchronous DB entry
import time
from Queue import Queue, Full, Empty
from threading import Thread, Lock, Event

# for multithreaded processing
from asyncjoin import AsyncThreadPool, AsyncThreadPoolMonitor, async

from django.db import connection, transaction

pool = AsyncThreadPool(16) # 16 worker threads
pool.spawn_threads()
monitor = AsyncThreadPoolMonitor(pool) # attach thread pool monitor

# multiple threads cause OperationalError('FATAL:  too many connections for role "kuehnel"\n',)
# remedy
# Pool DB commit operations!!!
db_committing_queue = Queue(5000)
kill_commit_worker = False

@transaction.commit_manually
def dbCommitWorker(min_size):
  global db_committing_queue
  global kill_commit_worker

  while not kill_commit_worker:
    must_commit = False
    if db_committing_queue.qsize() < min_size:
      time.sleep(2) # wait two seconcds
      if db_committing_queue.qsize() < min_size:
        time.sleep(1) # wait another 1 second

    try:
      for counter in range(20):
        my_model = db_committing_queue.get(False) # not a blocking get!
        my_model.save()
        must_commit = True
    except Empty:
      pass
    except Exception, e: # other db related errors
      print repr(e)
      continue
    finally:
      if must_commit:
        transaction.commit()

# start the commit worker
db_commit_thread = Thread(target=dbCommitWorker, args=(5,))
db_commit_thread.daemon=True
db_commit_thread.start()
