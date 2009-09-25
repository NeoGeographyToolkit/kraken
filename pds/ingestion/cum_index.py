#!/usr/bin/env python
'''A parser for PDS cumulative index files.  Given the filenames of a
label file and the corresponding table file, the Table object provides
an iterable interface to the entries in the index.  Each entry is a 
Table.Row object whose attributes are (lowercase versions of) the 
column names, as defined in the label file.  Example Usage:

    table = Table(label_file, table_file)
    for entry in table:
        print entry.product_id

If you are okay with reading the entire table into memory all at once 
and would prefer random access capability, just convert the table to 
a list like this:

    table = list(table)
'''

import os
import re
# parsing INDEX TAB files, we cannot rely on the comma separation!!!
#import csv
# handling time entries
from datetime import datetime

whitespace_re = re.compile(r'^\s*$')
simple_entry_re = re.compile(r'^\s*([A-Z_^]+)\s*=\s*([-A-Z_0-9.:]+)\s*$')
string_entry_re = re.compile(r'^\s*([A-Z_^]+)\s*=\s*("(.*))?$')
string_start_re = re.compile(r'^\s*"(.*)')
string_end_re = re.compile(r'^([^"]*)"\s*$')
end_re = re.compile(r'^END\s*$')
end_object_re = re.compile(r'^\s*END_OBJECT')

class Label(object):
    def _parse_string(self, source, first):
        if not first:
            first = source.readline()
        if not first:
            raise ValueError('Malformed PDS label: Unexpected end of file!')
        m = string_start_re.match(first)
        if not m:
            raise ValueError('Malformed PDS label: Expected string constant!')
        first = m.group(1)
        value = ""
        while True:
            if first:
                line = first
                first = None
            else:
                line = source.readline()
                if not line:
                    raise ValueError('Malformed PDS label: Unexpected end of file!')
            m = string_end_re.match(line)
            if m:
                value += m.group(1)
                return value
            else:
                value += line

    def __init__(self, source, endre=end_re):
        self._properties = {}
        self.objects = []
        while True:
            line = source.readline()
            if not line:
                raise ValueError('Malformed PDS label: Unexpected end of file!')
            if endre.match(line):
                return
            if whitespace_re.match(line):
                continue
            m = simple_entry_re.match(line)
            if m:
                if m.group(1) == 'OBJECT':
                    object = Label(source,end_object_re)
                    object.id = m.group(2)
                    self.objects.append(object)
                    continue
                self._properties[m.group(1)] = m.group(2)
                continue
            m = string_entry_re.match(line)
            if m:
                self._properties[m.group(1)]= self._parse_string(source,m.group(2))
                continue
            raise ValueError('Malformed PDS label: ' + line)

    def __getitem__(self,key):
        return self._properties[key]

    def keys(self):
        return self._properties.keys()
            
class Table(object):
    def __init__(self, labelfile, tablefile=None):
        # test whether we already have a file handle
        try:
          if isinstance(labelfile, basestring):
            lbl = Label(open(labelfile,'r'))
          else:
            lbl = Label(labelfile)

          if tablefile is None:
            carats = [key for key in lbl.keys() if key[0] == '^']
            tablefile = os.path.join( os.path.dirname(labelfile), lbl[carats[0]] )
            self.table = open(tablefile, 'r')
          elif isinstance(tablefile, basestring):
            self.table = open(tablefile, 'r')
          elif callable(getattr(tablefile, 'read', None)):
            self.table = tablefile
        except Exception, e:
          raise # something went wrong with the index files
        
        columns = lbl.objects[0].objects
        self._column_parsers = [self._column_parser(column) for column in columns]
        self.column_names = [column['NAME'].lower() for column in columns]

    def _string_entry(self,name,start,stop):
        return lambda val: (name, val[start:stop].strip())

    def _integer_entry(self,name,start,stop):
        def parser(val):
            try:
                return (name, int(val[start:stop]))
            except ValueError:
                return (name, val[start:stop].strip())
        return parser

    def _real_entry(self,name,start,stop):
        def parser(val):
            try:
                return (name, float(val[start:stop]))
            except ValueError:
                return (name, val[start:stop].strip())
        return parser

    def _time_entry(self,name,start,stop):
        def parser(val):
            # first split off microseconds if necessary
            time_str = val[start:stop].strip()
            time_fractional = None
            try:
                (time_reg, time_fractional) = time_str.rsplit('.')
            except ValueError:
                time_reg = time_str
            try:
                x = datetime.strptime(time_reg, "%Y-%jT%H:%M:%S")
            except ValueError:
                try:
                    x = datetime.strptime(time_reg, "%Y-%m-%dT%H:%M:%S")
                except ValueError:
                    return (name, time_str)
            # handle micro seconds if necessary
            tz = None # TODO: we assume UTC time!
            if time_fractional is not None:
                fracpower = 6 - len(time_fractional)
                fractional = float(time_fractional) * (10 ** fracpower)
                x.replace(microsecond=int(fractional), tzinfo=tz)
            return (name, x)
        return parser

    def _column_parser(self,column):
        type = column['DATA_TYPE']
        name = column['NAME'].lower()
        spos = int(column['START_BYTE'])-1
        slen = int(column['BYTES'])
        if type == 'ASCII_INTEGER':
            return self._integer_entry(name,spos,spos+slen)
        elif type == 'ASCII_REAL':
            return self._real_entry(name,spos,spos+slen)
        elif type == 'TIME':
            return self._time_entry(name,spos,spos+slen)
        else:
            return self._string_entry(name,spos,spos+slen)

    class Row(object):
        def __init__(self,table,row):
            kv_list = [parse(row) for parse in table._column_parsers]
            self.__dict__.update(kv_list)
            self.value_list = [v for (k,v) in kv_list]

    def __iter__(self):
        for row in self.table:
            yield Table.Row(self, row)
