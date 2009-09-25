from subprocess import Popen, PIPE

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
}