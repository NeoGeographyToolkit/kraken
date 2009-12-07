from subprocess import Popen, PIPE
import os.path
#TODO: find a home for utility methods like this
def which(command):
    return Popen(('which', command), stdout=PIPE).stdout.read().strip()

connection_params = {
    'host': "wwt10one:5672",
    'userid': "guest",
    'password': "guest",
    'virtual_host': "/",
    'insist': False
}
