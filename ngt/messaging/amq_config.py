from subprocess import Popen, PIPE
import os.path
#TODO: find a home for utility methods like this
def which(command):
    return Popen(('which', command), stdout=PIPE).stdout.read().strip()

connection_params = {
    'host': "localhost:5672",
    'userid': "guest",
    'password': "guest",
    'virtual_host': "/",
    'insist': False
}

commands = {
    'echo': which('echo'),
    'grep': which('grep'),
    'ls': which('ls'),
    #'test': os.path.join(os.path.split(__file__)[0], 'fake_mosiac.py'), #a test command that randomly fails
    'test': '/Users/ted/code/alderaan/ngt/messaging/fake_command.py',
    'size': which('du'),
}