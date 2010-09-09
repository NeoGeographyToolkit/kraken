#from command_pb2 import Command
#from status_pb2 import Status
from protocols_pb2 import RpcRequestWrapper, RpcResponseWrapper
import protocols_pb2 as protobuf # FOR REFACTOR: protobuf is a terrible alias name for this module and it's infected everything that uses it.
import logging
logger = logging.getLogger('protocol')

__all__ = ('test', 'dotdict', 'pack', 'unpack', 'RpcRequestWrapper', 'RpcResponseWrapper', 'protobuf')

class dotdict(dict):
    """ Dictionary subclass that gives you JavaScript-style dot-operator access to members. """
    def __getattr__(self, attr):
        return self.get(attr, None)
    __setattr__= dict.__setitem__
    __delattr__= dict.__delitem__

####
# A Simplified interface to google.protobuf
###

def pack(msgclass, data):
    """ Convert the data in dict 'data' to a protocol buffer Message of type 'msgclass'
        Returns the serialized message.
    """
    msg = msgclass()
    for k, v in data.items():
        try:
            assert hasattr(msg, k)
        except:
            raise AttributeError("%s doesn't have an attribute named %s." % (str(msgclass), k))
        field_descriptor = msg.DESCRIPTOR.fields_by_name[k]
        logger.debug("Packing %s (%s)" % (k, field_descriptor.label) )
        if field_descriptor.label == field_descriptor.LABEL_REPEATED:
            if not hasattr(v, '__iter__'):
                raise AssertionError("VALUE FOR REPEATED FIELD %s IS NOT AN ITERATOR: %s" % (field_descriptor.name, str(v)))
            field = getattr(msg, k)
            for i in v:
                field.append(i)
        else:
            assert field_descriptor.label in [field_descriptor.LABEL_OPTIONAL, field_descriptor.LABEL_REQUIRED]
            setattr(msg, k, v)
    assert msg.IsInitialized
    logger.debug("PACK %s --> %s" % (str(data), str(msg.SerializeToString())[:80]))
    return msg.SerializeToString()
            
    
def unpack(msgclass, msgstring):
    """ Takes a protocol buffer Message and return a dotdict containing its data"""
    message = msgclass()
    message.ParseFromString(msgstring)
    dd = dotdict()
    for field_descriptor, value in message.ListFields():
        fieldname = field_descriptor.name
        if field_descriptor.label == field_descriptor.LABEL_REPEATED:
            dd[fieldname] = list(value)
        else:
            dd[fieldname] = value
    logger.debug("UNPACK %s --> %s" % (msgstring[80:], str(dd)[80:]))
    return dd
            
    
def test():
    d = {'args': ['arg1', 'arg2'], 'command': 'foo', 'uuid': '0123456789ABCD'}
    pb = pack(Command, d)
    print "MSG: ", pb
    assert unpack(Command, pb) == d
    print "SUCCESS!"
    return unpack(Command, pb)
