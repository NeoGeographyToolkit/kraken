from command_pb2 import Command
from status_pb2 import Status

__all__ = ('test', 'dotdict', 'pack', 'unpack', 'Command', 'Status')

class dotdict(dict):
    """ Dictionary subclass that gives you JavaScript-style dot-operator access to members. """
    def __getattr__(self, attr):
        return self.get(attr, None)
    __setattr__= dict.__setitem__
    __delattr__= dict.__delitem__


def pack(msgclass, data):
    """ Convert a the data in dict 'data' to a protocol buffer Message of type 'msgclass'
        Returns the serialized message.
    """
    msg = msgclass()
    for k, v in data.items():
        assert hasattr(msg, k)
        field_descriptor = msg.DESCRIPTOR.fields_by_name[k]
        if hasattr(v, '__iter__'):
            assert field_descriptor.label == field_descriptor.LABEL_REPEATED # verify this is a repeated field
            field = getattr(msg, k)
            for i in v:
                field.append(i)
        else:
            assert field_descriptor.label in [field_descriptor.LABEL_OPTIONAL, field_descriptor.LABEL_REQUIRED]
            setattr(msg, k, v)
    assert msg.IsInitialized
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

    return dd
            
    
def test():
    d = {'args': ['arg1', 'arg2'], 'command': 'foo', 'uuid': '0123456789ABCD'}
    pb = pack(Command, d)
    print "MSG: ", pb
    assert unpack(Command, pb) == d
    print "SUCCESS!"
    return unpack(Command, pb)