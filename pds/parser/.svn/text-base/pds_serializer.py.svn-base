# Histrory (version, date, author/contributor, comments):
# v0.9,  9/26/07, Frank Kuehnel, initial version analyzes grammer and constructs hash table
# v0.95, 9/28/07, Frank Kuehnel, export YAML hash, access remote files via urllib2, improved hash building, ISIS cub file support
# v0.96, 10/2/07, Frank Kuehnel, some simplification, support for pointer statements
# v0.97, 10/4/07, Frank Kuehnel, same object identifier treated correctly, grammar bug fixes, full support for ptr stmt
# v0.98, 11/5/07, Frank Kuehnel, FTP socket support, cleanups, grammar corrections for qube files, single and double quoted strings
# v0.99, 11/19/07, Frank Kuehnel, sanatize filename for saving in yaml files
# v1.00, 12/02/07, Frank Kuehnel, improved parser grammar to work on Hirise PDS files
# v1.01, 02/15/08, Frank Kuehnel, corrected ODL characterset for double quotes, single quotes, time scanning issues, group statement
# v1.1, 01/30/09, Frank Kuehnel, adapted code for Harvestman integration and only file access sniffing! Readied for python 2.4
# v1.1.1 3/2/09, Frank Kuehnel, readied for PDS-Django SVN merger
# v1.2 8/31/09, Frank Kuehnel, code refactoring into PdsParser class
# Copyright, NASA/RIACS, NASA/SGT

import sys
import os
import time
import re
import string
import itertools

try:
  from simpleparse.common import strings
  from simpleparse.stt.TextTools import print_tags
  from simpleparse.parser import Parser
except ImportError:
  this_dir = os.path.dirname(__file__)
  sys.path.insert(0, os.path.abspath(this_dir+'/../../lib64/python'))
  from simpleparse.common import strings
  from simpleparse.stt.TextTools import print_tags
  from simpleparse.parser import Parser
  
BUFFER_SIZE = 8192

class PdsParseError(Exception):
  def __init__(self, msg):
    self.value = msg
  def __str__(self):
    return repr(self.value)
  def __repr__(self):
    return repr(self.value)

# a helper method
def group(lst, n):
  """group([0,3,4,10,2,3],2) => iterator

  Group an iterable into an n-tuples iterable. Incomplete tuples are discarded e.g.
  >>>list(group(range,10,3))
  [(0,1,2),(3,4,5),(6,7,8)]
  """
  return itertools.izip(*[itertools.islice(lst,i, None,n) for i in range(n)])

class PdsParser(object):

  # define the (strict?) PDS grammar
  pds3_grammar = r"""
pdscubelabel   := (pds3label/cubelabel)
sniffpdscubefile := (sniffpds3file/sniffcubefile)
>pds3label<      := c"pds_version_id",ws,'=',ws,c"pds3",ws,crlf,structure3
>sniffpds3file<  := c"pds_version_id",ws,'=',ws,c"pds3",ws,crlf,filedescriptor?
>cubelabel<      := c"ccsd3zf0000100000001njpl3if0pds200000001",ws,'=',ws,c"sfdu_label",ws,crlf,cubestruct
>sniffcubefile<  := c"ccsd3zf0000100000001njpl3if0pds200000001",ws,'=',ws,c"sfdu_label",ws,crlf,filedescriptor
# white space not reported (instantiated), 
<ws>           := spacechar*
# need both cr & lf for proper line termination, '\r\n', lax version [\r\n]+
<crlf>         := [\r\n]+
# PDS high level label structure
>structure3<   := (filedescriptor?,data_pointers?,rest_of_label)
>cubestruct<   := (filedescriptor,data_pointers,rest_of_label)
>filedescriptor< := ?-(ptr_stmt_line),(emptyline,asg_stmt_line)+
>data_pointers<:= (emptyline,ptr_stmt_line)+
>rest_of_label<:= (emptyline,statementline)+
<emptyline>    := (commentline/nullline)*
<nullline>     := ws,crlf
<commentline>  := ws,comment,ws,crlf
# PDS object, group & line structure
>statementline<:= ws,statement,ws,crlf
>asg_stmt_line<:= ws,assign_stmt,ws,crlf
>ptr_stmt_line<:= ws,pointer_stmt,ws,crlf
<comment>      := "/*",(?-'*/',ODLcharset)*,"*/"
>statement<    := ?-(end_object_stmt/end_group_stmt),(object_stmt/group_stmt/pointer_stmt/assign_stmt)
object_stmt    := c"object",ws,'=',ws,identifier,ws,crlf,(nullline/commentline/statementline)*,ws,end_object_stmt
<end_object_stmt> := c"end_object",ws,('=',ws,identifier)?
group_stmt     := c"group",ws,'=',ws,identifier,ws,crlf,(nullline/commentline/statementline)*,ws,end_group_stmt
<end_group_stmt> := c"end_group",ws,('=',ws,identifier)?
pointer_stmt   := '^',identifier,ws,'=',ws,value
assign_stmt    := ?-(c"object "/c"group "/c"end_object"/c"end_group"),nsidentifier,ws,'=',ws,value
nsidentifier   := identifier,(':',identifier)?
identifier     := letter, (letter/digit/('_',letter)/('_',digit))*
# PDS types of values, need this order!
>value<        := sequencevalue/setvalue/scalarvalue
sequencevalue  := sequence2D/sequence1Dunit/sequence1D
sequence2D     := '(',ws,sequence1D+,ws,')'
# this seems like wrong PDS grammar, but in the hirise examples there is a unit at the end of a tuple...?
sequence1D     := '(',ws,scalarvalue,ws,(',',(ws/crlf/ws),ws,scalarvalue,ws)*,')'
sequence1Dunit := sequence1D,ws,units_expr
setvalue       := '{',ws,scalarvalue,ws,(',',(ws/crlf/ws),ws,scalarvalue,ws)*,'}'
scalarvalue    := datetimevalue/numericvalue/textvalue/symbvalue
# PDS date time value, order is important for date time recognition
datetimevalue  := date_time/time/date
date_time      := date,'T',time
date           := (digit+,'-',digit+,'-',digit+) / (digit+,'-',digit+)
time           := zonetime/utctime/localtime
zonetime       := hms,[+-],digit+,(':',digit+,('.',digit+)?)?
utctime        := hms,'Z'
localtime      := hms
<hms>          := digit+,':',digit+,(':',unscaledreal/unsignedint)?
# PDS numeric value, order is important for number recognition
numericvalue   := realnumber/basedintnumber/intnumber
realnumber     := [+-]?,(scaledreal/unscaledreal),ws,(units_expr)?
<scaledreal>   := unscaledreal,('E'/'e'),intnumber
<unscaledreal> := ('.',unsignedint)/(unsignedint,'.',digit*)
intnumber      := [+-]?,unsignedint,ws,(units_expr)?
basedintnumber := (digit,digit?),'#',[+-]?,[0-9a-fA-F]+,'#',ws,(units_expr)?
<unsignedint>  := digit+
<signedint>    := [+-]?,digit+
# PDS units expression, Hirise PDS unites expression '<DAYS/24>' does not conform with PDS
units_expr     := '<',units_factor,([*/]?,(units_factor/signedint))*,'>'
units_factor   := identifier,('**',signedint)?
# PDS string and symbol values, double & single quoted strings
textvalue      := ('"',-[\042]+,'"')/("'",-[\047]+,"'")
symbvalue      := identifier
# PDS character set
<ODLcharset>   := (letter/digit/spechar/spacechar/otherchar)
<letter>       := [a-zA-Z/] # '/' was added because of some viking label inconsistencies
<digit>        := [0-9]
<spechar>      := [-+={}()<>.\042\047_,/*:#&^]
<otherchar>    := [!$%;?@`\133\135|~]
<spacechar>    := [ \t]
<formateff>    := [\n\r\v]
"""

  # a sniffing parser is used for finding data characteristics,
  # i.e. label size in bytes
  sniff_parser = Parser(pds3_grammar, "sniffpdscubefile")
  # parse the full pds label structure
  full_parser = Parser(pds3_grammar, "pdscubelabel")

  def __init__(self, data_stream, parser_lock=None):
    self.data = None
    self.pds_stream = data_stream
    self.parser_lock = parser_lock

  def scanValue(self, node):
    value = None
    try:
      (node_name, c_start, c_stop, sub_nodes) = node
      if (node_name == 'scalarvalue'):
        # analyze type of scalar value
        (sub_node_name, gc_start, gc_stop, great_grand_children) = sub_nodes[0]

        if (sub_node_name == 'numericvalue'):
          # what kind of numeric value is it?
          great_grand_child_name, ggc_start, ggc_stop, dummy = great_grand_children[0]
          try:
            # do we also have a unit expression?
            tmp_rep, tmp_start, tmp_stop, tmp_dummy = dummy[0]
            units_expr = self.data[tmp_start:tmp_stop]
            ggc_stop = tmp_start
          except IndexError, e:
            units_expr = ''	# no units available

          tmp = self.data[ggc_start:ggc_stop]
          if (great_grand_child_name == 'intnumber'):
            number = long(tmp) # this is an integer number
          elif (great_grand_child_name == 'realnumber'):
            number = float(tmp) # this is a float number
          elif (great_grand_child_name == 'basedintnumber'):
            try: # what is the integer base?
              cidx = tmp.index('#',1,3)
              radix = int(tmp[0:cidx])
              # calculate integer number
              number = int(tmp[cidx+1:tmp.rindex('#',cidx)], radix)
            except ValueError:
              number = 0
          else:
            PdsParseError, 'scanValue: do not understand numeric value \"%s\" ' % (tmp)
            number = 0

          if (units_expr == ''):
            value = number
          else:
            value = (number, units_expr) # value with units

        elif (sub_node_name == 'symbvalue'):
          symbvalue = self.data[gc_start:gc_stop].lower()
          value = symbvalue.strip('\'') # symbol values are all lower case, remove quotes
        elif (sub_node_name == 'datetimevalue'):
          value = self.data[gc_start:gc_stop] # for now, treat as plain text string value
        else:
          textval = self.data[gc_start:gc_stop] # just treat as plain text string value, remove quotes?
          value = textval.strip('"').strip("\'")

      elif (node_name == 'sequencevalue'):
        value = []
        (sub_node_name, gc_start, gc_stop, great_grand_children) = sub_nodes[0]
        # recursively add values
        if (sub_node_name == 'sequence1D'):
          for great_grand_child in great_grand_children:
            value.append(self.scanValue(great_grand_child))
        elif (sub_node_name == 'sequence1Dunit'):
          unit_report, unit_start, unit_stop, unit_subtree = great_grand_children[1]
          unit_name = self.data[unit_start:unit_stop]
          for great_grand_child in great_grand_children[0][3]:
            value.append((self.scanValue(great_grand_child), unit_name))
        else:
          raise PdsParseError, 'scanValue: PDS sequence2D values are not yet supported'

      elif (node_name == 'setvalue'):
        value = []
        for sub_node in sub_nodes:
          value.append(self.scanValue(sub_node))

    except Exception, e:
      #print repr(e)
      pass

    finally: # catch all exceptions and return a value or None
      return value

  def constructHash(self, parser_tree):
    kv_list      = [] # the flat ususal keyword value list
    object_list  = [] # objects represent a keyword value list in a group
    pointer_list = [] # pointers are treated special

    # iterate over parser results
    for (report, start, stop, children) in parser_tree:
      try:
        child_report, c_start, c_stop, grand_children = children[0]
        name = self.data[c_start:c_stop].lower() # data object identifier lower case
      except:
        continue

      if (report == 'assign_stmt'):
        value = self.scanValue(children[1])
        if (value is not None):
          kv_list.append((name.replace(':','#'), value)) # substitute ':' with '#'
      elif (report == 'object_stmt' or report == 'group_stmt'):
        # recursively add a sub keyword-value list
        value = self.constructHash(children[1:])
        object_list.append((name, value))
      elif (report == 'pointer_stmt'):
        value = self.scanValue(children[1])
        if (value is not None):
          pointer_list.append((name, value))

    # construct & return the hash table
    hash = dict(kv_list)

    if len(object_list) > 0:
      #	object_list.sort(lambda e1,e2: cmp(e1[0],e2[0])) # sort according to keys, don't need this?
      for (key, value) in object_list:
        try:
          hash[key].append(value)
        except KeyError:
          hash.setdefault(key, value)
        except AttributeError:
          tmp = hash[key]
          hash[key] = [tmp, value]

    if len(pointer_list) > 0:
      for (key, value) in pointer_list:
        try:
          hash[key].setdefault('dataptr', value)
        except KeyError:
          hash.setdefault(key, {'dataptr':value})

    return hash

  # returns a key value hash upon success, otherwise throws an Exception
  def process(self, filename, is_viking_edr=False):
    self.data = self.pds_stream.read(BUFFER_SIZE) # sniff into header

    # cleanup header data, necessary for Viking EDR data!
    if is_viking_edr:
      tmp = re.sub('\x00(.|\x0a)\x00', '\n', self.data[2:])
      self.data = re.sub('[\x00\x03\x0a\x24-\x44]\x00', '\n', tmp)

    if self.parser_lock is not None: # parser lock is needed for multithreaded python use!
      self.parser_lock.acquire()
    success, root_node, next = PdsParser.sniff_parser.parse(self.data)
    if self.parser_lock is not None:
      self.parser_lock.release()

    if not success: # close channels and sockets if neccessary
      raise  PdsParseError, "cannot read PDS header for \'%s\'" % filename

    # create keyword value list from parse result
    kv_list = self.constructHash(root_node)
    try:
      try:
        (label_length, byteunit) = kv_list['label_records']
        if byteunit not in ('<BYTES>','<BYTE>'):
          raise TypeError
      except TypeError:
        label_length = int(kv_list['record_bytes']*kv_list['label_records'])
        if label_length <= 0:
          raise ValueError
    except:
      base, sep, ext = filename.rpartition('.')
      if ext.lower() in ['lbl','cat','lab']:
        label_length = sys.maxint
      else:
        raise PdsParseError, "cannot determine header size for \'%s\'" % filename

    # now, read the entire pds/qube label into memory
    if label_length > BUFFER_SIZE:
      self.pds_stream.seek(0)
      if label_length < sys.maxint:
        self.data = self.pds_stream.read(label_length)
      else:
        self.data = self.pds_stream.read()
      # cleanup data, necessary for Viking EDR data!
      if is_viking_edr:
        tmp = re.sub('\x00(.|\x0a)\x00', '\n', self.data[2:])
        self.data = re.sub('[\x00\x03\x0a\x24-\x44]\x00', '\n', tmp)

    time_start = time.time()
    if self.parser_lock is not None: # parser lock is needed for multithreaded python use.
      self.parser_lock.acquire() # mxTools is not thread safe!!!
    success, root_node, next = PdsParser.full_parser.parse(self.data)
    if self.parser_lock is not None:
      self.parser_lock.release()
    d = time.time()-time_start

    if not success:
      raise PdsParseError, "scanning PDS label file \'%s\' failed!" % filename

    # sys.stdout.write("parsed %s characters of %s in %s seconds (%scps)\n" %(next, len(pds_label), d, next/(d or 0.000000001)))
    # sys.stdout.write('label analysis:\n')
    # print_tags(pds_label, root_node)
    return self.constructHash(root_node)
