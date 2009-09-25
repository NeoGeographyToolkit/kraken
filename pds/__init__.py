import sys, os
# prepend library search path
root_configuration= os.path.dirname(__file__)
sys.path.insert(0, os.path.abspath(root_configuration+'/lib'))

import feeds
