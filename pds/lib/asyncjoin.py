"""
asynchronous join calculus module, see usage example below for utilization.
Also see license below.
"""

import sys
import trace
import math
import time
import threading
import random
from Queue import Queue, Full, Empty
from decorator import decorator, getinfo
import inspect

class synchronized(object):
  """ Class enapsulating a lock and a function
  allowing it to be used as a synchronizing
  decorator making the wrapped function
  thread-safe """
    
  def __init__(self, *args):
    self.lock = threading.Lock()
        
  def __call__(self, f):
    def lockedfunc(*args, **kwargs):
      self.lock.acquire()
      try:
        return f(*args, **kwargs)
      except Exception, e:
        raise
      finally:
        self.lock.release()

    return lockedfunc

class DuplicateTaskError(Exception):
  """ Interrupt raised task is already scheduled """
  def __init__(self, value):
    self.value = value
  def __str__(self):
    return repr(self.value)
  def __repr__(self):
    return '%s for: %s' % (self.__class__.__name__,self)

class AsyncThreadInterrupt(Exception):
  """ Interrupt raised to kill a AsyncThread class's object """
  def __init__(self, value):
    self.value = value
  def __str__(self):
    return str(self.value)

class TaskObject(object):
  """ Task wrapper for pooled tasks """

  def __init__(self, func, args, kw, task_key=None, timeout=0, max_try=0):
    self.func = func
    self.args = args
    self.kw = kw
    self.timeout = timeout
    self.max_try = max_try
    self.task_key = task_key
    self.current_try = 0

  def __str__(self):
    return self.func.__name__

  def get_key(self):
    """ Return the task key """
    return self.task_key

  def get_method_call(self):
    """ Return the method call and arguments """
    return self.func, self.args, self.kw

  def get_timeout(self):
    return self.timeout

  def increment_try(self):
    self.current_try = self.current_try + 1

  def continue_trying(self):
    """ Check whether we should do more tries """
    return bool(self.current_try < self.max_try)

class AsyncThread(threading.Thread):
  """ Class to handle tasks in a separate thread, thread can be killed at any python statement """

  # The last error which caused a thread instance to die
  _lasterror = None

  def __init__(self, name, threadpool):
    """ Constructor, the constructor takes a url, a filename
      , a timeout value, and the thread pool object pooling this
      thread """
    # method Object
    self._taskobject = None
    # thread queue object pooling this thread
    self._pool = threadpool
    # max lifetime for the task of this thread
    self._timeout = 0
    # start time of thread, may include waiting time
    self._starttime = 0
    # busy flag
    self._busyflag = False
    # end flag
    self._endflag = False
    # initialize threading
    threading.Thread.__init__(self, None, None, name)

  def __str__(self):
    return self.getName()

  def start(self):
    """ start the thread, install the trace, this makes threads killable """
    self.__run_backup = self.run
    self.run = self.__run # force thread to install our trace
    threading.Thread.start(self)

  def __run(self):
    """ Hack run function, which installs the trace """
    sys.settrace(self.globaltrace)
    self.__run_backup()
    self.run = self.__run_backup

  def globaltrace(self, frame, why, arg):
    if why == 'call':
      return self.localtrace
    else:
      return None

  def localtrace(self, frame, why, arg):
    if self._endflag and why== 'line':
      raise SystemExit() # equivalent to sys.exit()
      return self.localtrace

  def is_busy(self):
    """ Get busy status for this thread """
    return self._busyflag

  def join(self):
    """ The thread's join method to be called
        by other threads """
    threading.Thread.join(self, self._timeout)

  def terminate(self):
    """ Kill this thread """
    self.stop()
    msg = 'Task thread, ' + self.getName() + ' killed!'
    raise AsyncThreadInterrupt, msg

  def stop(self):
    """ Stop this thread """
    # If task was not completed, push-back object to the pool if not exceeding retries.
    if self._busyflag and self._taskobject:
      self._taskobject.increment_try()
      if self._taskobject.continue_trying():
        print 'push task back into pool'
        self._pool.push(self._taskobject)

    self._endflag = True

  def run(self):
    """ Run this thread """
    while not self._endflag:
      try:
        # print 'Waiting for next task',self
        # this can be a blocking call!
        self._taskobject = None
        task_obj = self._pool.get_next_task()

        if not isinstance(task_obj, TaskObject):
          time.sleep(0.1) # put in some delay
          continue
        else:
          self._timeout = task_obj.get_timeout()

        # Dont do duplicate checking for multipart...
        #if not url_obj.trymultipart and self._pool.check_duplicates(url_obj):
        #  print 'Is duplicate',url_obj.get_full_url()
        #  continue

        # set busy flag to 1, start counting task time
        self._starttime=time.time()
        self._busyflag = True

        # Save reference
        self._taskobject = task_obj
        func, args, kwargs = task_obj.get_method_call()

        # Perf fix: Check end flag
        # in case the program was terminated
        # between start of loop and now!
        if not self._endflag:
          func(*args, **kwargs)

      except Exception, e:
        raise
        error('Worker thread Exception',e)
        # Now I am dead - so I need to tell the pool
        # object to migrate my data and produce a new thread.
            
        # See class for last error. If it is same as
        # this error, don't do anything since this could
        # be a programming error and will send us into
        # a loop...
        if str(self.__class__._lasterror) == str(e):
          debug('Looks like a repeating error, not trying to restart worker thread %s' % (str(self)))
        else:
          self.__class__._lasterror = e
          #self._pool.dead_thread_callback(self)
          error('Worker thread %s has died due to error: %s' % (str(self), str(e)))
          error('Worker thread was working on %s' % (str(self._taskobject)))

      finally:
        # clean up
        #print 'clean up thread'
        # set busyflag to False
        self._busyflag = False
        # remove key from threadpool, schedule_dict
        if self._taskobject is not None:
          key = self._taskobject.get_key()
          if key:
            self._pool.schedule_dict.pop(key)

  def get_task_elapsed_time(self):
    """ Get the time taken for the task of this thread, exclusive waiting """
    now=time.time()
    processtime=float(math.ceil((now-self._starttime)*100)/100)
    return processtime

  def long_running(self):
    """ Find out if this task thread is running for a long time
    (more than given timeout) """
    # if any thread task is running for more than <timeout>
    # time, return TRUE
    return self._busyflag and (self._timeout > 0) and (self.get_task_elapsed_time() > self._timeout)

class AsyncThreadPool(Queue):
  """ Thread group/pool class to manage asynchronous tasks """

  def __init__(self, numworker):
    assert numworker > 0, 'numworker > 0 required for AsyncThreadPool class'
    # list of spawned threads
    self._threads = []
    # Maximum number of threads spawned
    self._numthreads = numworker
    # Local buffer
    self.buffer = []
    # Condition object
    self._cond = threading.Condition(threading.Lock())
    # Monitor object, used with hget
    self._monitor = None
    # dictionary for current tasked scheduled
    self.schedule_dict = {}
    
    Queue.__init__(self, self._numthreads + 50)

  def spawn_threads(self):
    """ Start the processing threads """
    for i in range(self._numthreads):
      name = 'Worker-'+ str(i+1)
      worker = AsyncThread(name, self)
      worker.setDaemon(True)
      # Append this thread to the list of threads
      self._threads.append(worker)
      # print 'Starting thread',fetcher
      worker.start()

  def is_scheduled(self, key):
    """ Check if task with key is already in queue, or in process """
    return self.schedule_dict.has_key(key)

  def push(self, task):
    """ Push the task object """

    # Wait till we have a thread slot free, and push the
    # current task info when we get one
    key = task.get_key()
    if key is not None:
      self.schedule_dict[key] = 'wait'
    try:
      self.put(task, False) # non-blocking
    except Full:
      self.buffer.append(task)

  def get_next_task(self):
    # Insert a random sleep in range
    # of 0 - 0.5 seconds
    # time.sleep(random.random()*0.5)
    try:
      if len(self.buffer):
        # Get last item from buffer, buffer has priority over queue!
        task = self.buffer.pop() # non-blocking call
      else:
        # print 'Waiting to get item', threading.currentThread()
        task = self.get(True) # blocking call

      key = task.get_key()
      if key:
        self.schedule_dict[key] = 'run'

      return task

    except Empty:
      return None

  def end_hanging_threads(self):
    """ If any thread task is running for too long,
    kill it, and remove it from the thread pool """
    pool=[]
    for thread in self._threads:
      if thread.long_running(): pool.append(thread)

    try:
      self._cond.acquire()
      for thread in pool:
        #extrainfo('Killing hanging thread ', thread)
        # kill it
        try:
          thread.terminate()
        except AsyncThreadInterrupt:
          pass
        # create new thread in replacement
        new_thread = AsyncThread(thread.getName(), self)
        if new_thread:
          print 'create thread in replacement', new_thread.getName()
          idx = self._threads.index(thread)
          self._threads[idx] = new_thread
          new_thread.setDaemon(True)
          new_thread.start()
        else:
          # remove this thread from the thread list
          self._threads.remove(thread)

        print 'delete old thread', thread
        del thread
    finally:
      self._cond.release()

  def end_all_threads(self):
    """ Kill all running threads """
    try:
      self._cond.acquire()
      for t in self._threads:
        try:
          t.terminate()
          t.join()
        except AsyncThreadInterrupt, e:
          debug(str(e))
          pass

        self._threads = []
    finally:
      self._cond.release()
    
  def get_threads(self):
    """ Return the list of thread objects """
    return self._threads

class AsyncThreadPoolMonitor(threading.Thread):

  def __init__(self, threadpool):
    self._pool = threadpool
    self._pool._monitor = self
    self._endflag = False
    # initialize threading
    threading.Thread.__init__(self, None, None, "Monitor")        
    self.setDaemon(True)
    self.start()

  def run(self):
    while not self._endflag:
      self._pool.end_hanging_threads()
      time.sleep(1.0)

  def stop(self):
    self._endflag = True

def async(delay=0, threadpool=None, timeout=0, max_try=0):
  def call(func, *args, **kw):
    # check for duplicate tasks before pushing to task pool
    key = kw.pop('task_key', None)
    if issubclass(threadpool.__class__, AsyncThreadPool):
      if key and threadpool.is_scheduled(key):
        raise DuplicateTaskError, key
      threadpool.push(TaskObject(func, args, kw, task_key=key, timeout=timeout, max_try=max_try))
    else:
      thread = threading.Timer(delay, func, args, kw)
      thread.start()
      return thread
  return decorator(call)

# a decorator class to implement the asynchronous join calculus
class AsyncJoin:
  def __init__(self, threadpool=None, timeout=0, max_try=0):
    self.eventQueues = {}
    self.eventQueuesList = []
    self.tpool = threadpool if issubclass(threadpool.__class__,AsyncThreadPool) else None
    self.timeout = timeout
    self.max_try = max_try
    self.joinFunc = None

  # create a new function A from a closure wrapper A, for the proper context of the original function A
  def _create_wrapper(self, wrapper, model):
    infodict =  getinfo(model) # context of function A
    src = "lambda %(signature)s: _wrapper_(%(signature)s)" % infodict
    infodict['globals']['_wrapper_'] = wrapper
    newwrapper = eval(src, infodict['globals'], dict(_wrapper_=wrapper)) # create the non closure function with the proper context
    try:
      newwrapper.__name__ = infodict['name']
    except: # Python version < 2.4
      pass
    newwrapper.__doc__ = infodict['doc']
    newwrapper.__module__ = infodict['module']
    newwrapper.__dict__.update(infodict['dict'])
    newwrapper.func_defaults = infodict['defaults']
    newwrapper.undecorated = model
    newwrapper.join_instance = self
    return newwrapper

  def addJoinFunction(self, joinFunction):
    infodict = getinfo(joinFunction)
    # analyze arguments and signature
    if infodict['argnames'] and infodict['signature']:
      self.joinFunc = joinFunction

  def newEventQueue(self, name):
    if self.eventQueues.has_key(name):
      assert False, 'duplicative join function name \'%s\'' % name
    else:
      queue = Queue(100)
      self.eventQueuesList.append(queue)
      self.eventQueues.setdefault(name, queue)    

  def __call__(self, initialFunc):
    self.newEventQueue(initialFunc.__name__)
    return self._create_wrapper(lambda *a, **kw: self.eventTrigger(initialFunc, *a, **kw), initialFunc)

  @synchronized()
  def getJoinArguments(self):
    try:
      for queue in self.eventQueuesList:
        if queue.empty():
          return None
      # TODO: assemble arguments with the proper signature for join function
      argsJ = []
      kwJ = {}
      for queue in self.eventQueuesList:
        args, kw = queue.get(False) # non blocking call
        argsJ.extend(args)
        kwJ.update(kw)

      return (argsJ, kwJ)

    except: # some error occured?
      return None
    
  def eventTrigger(self, func, *args, **kw): # event for func is triggered
    if self.joinFunc is None:
      assert False, 'join function body not yet defined.'
    else:
      #print 'event \'%s\' occured with arguments: %s, and keyword arguments %s' % (func.__name__, args, kw)
      self.eventQueues[func.__name__].put((args, kw)) # put into proper event queue
      result = self.getJoinArguments()
      if result:
        (argsJ, kwJ) = result
        #print 'async execute join func with event \'%s\' triggerd' % func.__name__
        #print 'join arguments', argsJ, kwJ
        if self.tpool:
          self.tpool.push(TaskObject(self.joinFunc, argsJ, kwJ, self.timeout, self.max_try)) # asynchronous join execution via thread pool
        else:
          thread = threading.Timer(0, self.joinFunc, argsJ, kwJ) # asynchronous join execution via new thread
          thread.start()
      else:
        pass
        # print 'some other event queue is still empty, queued up event \'%s\'' % func.__name__
        return

def withAsync(prevPartialHeader):
  try:
    join_inst = prevPartialHeader.__dict__['join_instance']
    if join_inst.__class__ != AsyncJoin:
      raise
  except:
    assert False, "\'%s\' needs to be decorated with @AsyncJoin" % prevPartialHeader.__name__

  # TODO: preserve proper signature of caller function, see decorator!
  def addJoin(nextPartialHeader):
    try:
      infodict = getinfo(nextPartialHeader)
      # modify async_instance, construct a join function
      join_inst.newEventQueue(infodict["name"])
      try:
        func = nextPartialHeader(*infodict["argnames"]) # do we have a nested joinFunction?
        if inspect.ismethod(func) or inspect.isfunction(func): # found a join function
          join_inst.addJoinFunction(func)
        else:
          raise
      except:
        pass
        # print 'no join function found, try adding another partial header!'

      def call(*a, **kw):
        join_inst.eventTrigger(nextPartialHeader, *a, **kw)

      return call
    except:
      assert False, "\'%s\' does not define a join function" % nextPartialHeader.__name__

  return addJoin

################
# USAGE EXAMPLE
################

if __name__ == '__main__':

  import time

  pool = AsyncThreadPool(3)
  pool.spawn_threads()
  monitor = AsyncThreadPoolMonitor(pool) # attach thread pool monitor!

  @async(threadpool=pool)
  def sayHello(what, **kw): # we need kw for supplying the task key
    print 'hello', what
    time.sleep(2)

  @async(threadpool=pool, timeout=2.0, max_try=2)
  def failedExample(value):
    print 'I am too tardy to show my value'
    time.sleep(5)
    print 'My value is', value

  @AsyncJoin(pool)
  def issue(request):
    pass
  @withAsync(issue)
  def reply(answer):
    pass
    # use a nested function to make it inaccessible (invisible) for the outside world
    def joinFunction(request, answer):
      print 'issued request:', request
      print 'and got answer:', answer
    return joinFunction

  print 'async example'
  try:
    sayHello('one', task_key='test')
    sayHello('two', task_key='test')
  except DuplicateTaskError, e:
    print '%s' % repr(e)

  sayHello('three')
  print 'go to sleep'
  time.sleep(3)
  sayHello('three', task_key='test')
  
  print 'timed out async example'
  failedExample('one')
  time.sleep(6)
  
  print 'async join example'
  issue('get response')
  time.sleep(1)
  reply('hello')
  time.sleep(6)

# Copyright 2008, Frank Kuehnel

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, ither express or implied.
# See the License for the specific language governing permissions
# and limitations under the License.