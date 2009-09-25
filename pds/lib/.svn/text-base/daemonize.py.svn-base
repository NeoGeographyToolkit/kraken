import os
import sys
from signal import SIGTERM
import time

class Daemon(object):
	
	def __init__(self, stdout = '/dev/null', stderr = None, stdin = '/dev/null', \
			pidfile = None, startmsg = 'started with pid %s'):
		self.stdout = stdout
		self.stderr = stderr
		self.stdin  = stdin
		self.pidfile = pidfile
		self.startmsg = startmsg

	# daemonize process
	# do the UNIX double-fork magic, see Stevens' "Advanced 
	# Programming in the UNIX Environment" for details (ISBN 0201563177)
	def daemonize():
		try:
			pid = os.fork()
			if pid > 0:
				# exit first parent
				sys.exit(0)
		except OSError, e:
			sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
			sys.exit(1)

		# decouple from parent environment
		os.chdir("/")
		os.umask(0)
		os.setsid()

		# do second fork
		try:
			pid = os.fork()
			if pid > 0:
				# exit from second parent
				sys.exit(0)
		except OSError, e:
			sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
			sys.exit(1)

		# open file descriptors and print start message
		if not self.stderr:
			self.stderr = self.stdout

		si = file(self.stdin, 'r')
		so = file(self.stdout, 'a+')
		se = file(self.stderr, 'a+', 0)
		pid = str(os.getpid())
		sys.stderr.write("\n%s\n" % self.startmsg % pid)
		sys.stderr.flush()
		if self.pidfile:
			file(self.pidfile,'w+').write("%s\n" % pid)

		# re-direct input output
		os.close(sys.stdout.fileno())
		os.close(sys.stderr.fileno())
		os.dup2(si.fileno(), sys.stdin.fileno())
		os.dup2(so.fileno(), sys.stdout.fileno())
		os.dup2(se.fileno(), sys.stderr.fileno())

	def startstop(action):
		try:
			pf  = file(self.pidfile,'r')
			pid = int(pf.read().strip())
			pf.close()
		except IOError:
			pid = None

		if ((action == 'stop') or (action == 'restart')):
			if (not pid):
				mess = "cannot stop daemon process, pid-file '%s' is missing.\n"
				sys.stderr.write(mess % pidfile)
				sys.exit(1)

			try:
				while 1:
					os.kill(pid, SIGTERM)
					time.sleep(1)
			except OSError, err:
				err = str(err)
				if err.find("No such process") > 0:
					os.remove(self.pidfile)
					if 'stop' == action:
						sys.exit(0)
					action = 'start'
					pid = None
				else:
					print str(err)
					sys.exit(1)

		if (action == 'start'):
			if (pid):
				mess = "daemon-pid-file '%s') already exists.\n"
				sys.stderr.write(mess % pidfile)
				sys.exit(1)

			self.daemonize()
			return

	print "syntax: %s start|stop|restart" % sys.argv[0]
	sys.exit(1)
