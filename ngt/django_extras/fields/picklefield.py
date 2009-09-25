from django.db import models
try:
    import cpickle as pickle
except ImportError:
    print "Couldn't locate cpickle.  Using the native python pickle."
    import pickle
    
class PickledDescriptor(property):
    def __init__(self, field):
        self.field = field
    
    def __get__(self, instance, owner):
        if not instance:
            return self
            
        if self.field.name not in instance.__dict__:
            data = getattr(instance, self.field.attname)
            instance.__dict__[self.field.name] = self.field.unpickle(data)

        return instance.__dict__[self.field.name]
        
    def __set__(self, instance, value):
        if self.field.readonly:
            raise 'Tried to set the readonly field "%s".' % self.field.name
        instance.__dict__[self.field.name] = value
        setattr(instance, self.field.attname, self.field.pickle(value))
    
class PickledObjectField(models.TextField):
    
    def __init__(self, readonly=False, *args, **kwargs):
        super(PickledObjectField, self).__init__(*args, **kwargs)
        self.readonly = readonly
    
    def pickle(self, obj):
        return pickle.dumps(obj)
    
    def unpickle(self, data):
        try:
          return pickle.loads(str(data))
        except:
          return None

    def get_attname(self):
        return '%s_pickled' % self.name

    def get_db_prep_save(self, value):
        "Returns field's value prepared for saving into a database."
        if isinstance(value, basestring):
          return value
        else:
          return self.pickle(value)

    def get_db_prep_lookup(self, lookup_type, value):
        raise ValueError("Can't compare pickled data.")
        
    def contribute_to_class(self, cls, name):
        super(PickledObjectField, self).contribute_to_class(cls, name)
        setattr(cls, name, PickledDescriptor(self))
