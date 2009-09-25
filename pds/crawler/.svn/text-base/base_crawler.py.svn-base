import socket
import urllib2
import base64
import cookielib
import cPickle as pickle
from cStringIO import StringIO
from datetime import datetime
import time

""" PdsCrawler.py - Module to manage and retrieve data
    from an internet connection using urllib2. This module contains
    code that is modified from the HarvestMan program."""

class UrlRetrievalError(object):
  """ Class encapsulating errors raised by PdsCrawler objects
  while connecting and downloading data from the Internet """

  def __init__(self):
    self.reset()

  def __str__(self):
    """ Returns string representation of an instance of the class """
    return ''.join((str(self.errclass),' ', str(self.number),': ',self.msg))

  def reset(self):
    """ Resets attributes """
    self.number = 0
    self.msg = ''
    self.fatal = False
    self.errclass = ''

class PdsCrawler(object):

  def __init__(self, logFileName):
    try:
      self.logFile = open(logFileName, 'w')
      self.logFile.write("%s start crawling\n" % now().strftime("%H:%M:%S on %Y-%m-%d"))
    except IOError, e:
      self.logFile = None
      raise Exception, "cannot open log file '%s'" % logFileName

  def finish(self):
    if self.logFile:
      self.logFile.write("%s done\n" % 'now')
      self.logFile.close()

  def configure(self, username=None, password=None):

    self.username = username
    self.passwd = password

    authhandler = urllib2.HTTPBasicAuthHandler()
    cookiehandler = None

    socket.setdefaulttimeout( self._cfg.socktimeout )
    cj = cookielib.MozillaCookieJar()
    cookiehandler = urllib2.HTTPCookieProcessor(cj)

    # HTTP handlers
    httphandler = urllib2.HTTPHandler
    
    opener = urllib2.build_opener(authhandler,
      urllib2.HTTPRedirectHandler,
      httphandler,
      urllib2.FTPHandler,
      #urllib2.GopherHandler,
      urllib2.FileHandler,
      urllib2.HTTPDefaultErrorHandler,
      cookiehandler)
    opener.addheaders=[] #Need to clear default headers so we can apply our own

    urllib2.install_opener(opener)

  def create_request(self, urltofetch, lmt='', etag='', useragent=True):
    """ Creates request object for the URL 'urltofetch' and return it """

    # This function takes care of adding any additional headers
    # etc in addition to creating the request object.

    # create a request object
    if lmt or etag:
      # Create a head request...
      request = HeadRequest(urltofetch)
      if lmt != '':
        ts = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.localtime(lmt))
        request.add_header('If-Modified-Since', ts)
      if etag != '':
        request.add_header('If-None-Match', etag)
    else:
      request = urllib2.Request(urltofetch)

      # Some sites do not like User-Agent strings and raise a Bad Request
      # (HTTP 400) error. Egs: http://www.bad-ischl.ooe.gv.at/. In such
      # cases, the connect method, sets useragent flag to False and calls
      # this method again.
      # print 'User agent', self._cfg.USER_AGENT
      if useragent: 
        request.add_header('User-Agent', self._cfg.USER_AGENT)

      # Check if any HTTP username/password are required
      username, password = self.username, self.passwd
      if username and password:
        # Add basic HTTP auth headers
        authstring = base64.encodestring('%s:%s' % (username, password))
        request.add_header('Authorization','Basic %s' % authstring)

    # If we accept http-compression, add the required header.
    if self._cfg.httpcompress:
      request.add_header('Accept-Encoding', 'gzip')

    return request

  def get_url_data(self, url):
    """ Downloads data for the given URL and returns it """
    try:
      urltask = UrlDataRetrieval()
      # asynchronous retrieval
      res = urltask.connect(urlobj)
      return urltask.get_data()
    except UrlRetrievalError, e:
      error("URL Error: ",e)
  
  class UrlDataRetrieval(object):
    """ Class which performs the work of fetching data for URLs
    from the Internet and saves data in memory."""

    def __init__(self):
      # # file like object returned by
      # urllib2.urlopen(...)
      self.fobj = None
      # data downloaded
      self.data = StringIO()
      # length of data downloaded
      self.datalen = 0
      # error object
      self.error = UrlRetrievalError()
      # Http header for current connection
      self.headers = CaselessDict()
      # Elasped time for reading data
      self.elapsed = 0.0
      # Status of connection
      # 0 => no connection
      # 1 => connected, download in progress
      self.status = 0

    def connect(self, urlobj, resuming=False):

      # Reset the http headers
      self.headers.clear()
      retries = self._cfg.retryfailed
      self._numtries = 0

      urltofetch = urlobj.get_full_url()    
      errnum = 0
      try:
        # Reset error
        self.error.reset()

        request = create_request(urltofetch)
        self.fobj = urllib2.urlopen(request)
        # Set status to 1
        self.status = 1
        # Set http headers
        set_http_headers()

        clength = int(self.get_content_length())
        if urlobj: urlobj.clength = clength
        encoding = self.get_content_encoding()
        clength = self.get_content_length()

        data = self.fobj.get_data()
        self.datalen = len(self.data)

        # Save a reference
        data0 = data
        self.fobj.close()

        if encoding.strip().find('gzip') != -1:
          try:
            gzfile = gzip.GzipFile(fileobj=cStringIO.StringIO(data))
            data = gzfile.read()
            gzfile.close()
          except (IOError, EOFError), e:
            data = data0
            pass
        else:
          self._datalen = self._fo.get_datalen()
          dmgr.update_bytes(self._datalen)

      except MemoryError, e:
        # Catch memory error for sockets
        pass          
        break
              
      except urllib2.HTTPError, e:
        try:
          errbasic, errdescn = (str(e)).split(':',1)
          parts = errbasic.strip().split()
          self._error.number = int(parts[-1])
          self._error.msg = errdescn.strip()
          self._error.errclass = "HTTPError"                    
        except:
          pass

        if self._error.msg:
          extrainfo(self._error.msg, '=> ',urltofetch)
        else:
          extrainfo('HTTPError:',urltofetch)

        try:
          errnum = int(self._error.number)
        except:
          pass

        if errnum==304:
          # Page not modified
          three_oh_four = True
          self._error.fatal = False
          # Need to do this to ensure that the crawler
          # proceeds further!
          content_type = self.get_content_type()
          urlobj.manage_content_type(content_type)                    
          break
        if errnum in range(400, 407):
          # 400 => bad request
          # 401 => Unauthorized
          # 402 => Payment required (not used)
          # 403 => Forbidden
          # 404 => Not found
          # 405 => Method not allowed
          # 406 => Not acceptable
          self._error.fatal = True
        elif errnum == 407:
          # Proxy authentication required
          self._proxy_query(1, 1)
        elif errnum == 408:
          # Request timeout, try again
          pass
        elif errnum == 412:
          # Pre-condition failed, this has been
          # detected due to our user-agent on some
          # websites (sample URL: http://guyh.textdriven.com/)
          self._error.fatal =  True
        elif errnum in range(409, 418):
          # Error codes in 409-417 contain a mix of
          # fatal and non-fatal states. For example
          # 410 indicates requested resource is no
          # Longer available, but we could try later.
          # However for all practical purposes, we
          # are marking these codes as fatal errors
          # for the time being.
          self._error.fatal = True
        elif errnum == 500:
          # Internal server error, can try again
          pass
        elif errnum == 501:
          # Server does not implement the functionality
          # to fulfill the request - fatal
          self._error.fatal = True
        elif errnum == 502:
          # Bad gateway, can try again ?
          pass
        elif errnum in (503, 506):
          # 503 - Service unavailable
          # 504 - Gatway timeout
          # 505 - HTTP version not supported
          self._error.fatal = True

        if self._error.fatal:
          rulesmgr.add_to_filter(urltofetch)                

      except urllib2.URLError, e:
        errdescn = ''
        self._error.errclass = "URLError"
          
        try:
          errbasic, errdescn = (str(e)).split(':',1)
          parts = errbasic.split()                            
        except:
          try:
            errbasic, errdescn = (str(e)).split(',')
            parts = errbasic.split('(')
            errdescn = (errdescn.split("'"))[1]
          except:
            pass

              try:
                  self._error.number = int(parts[-1])
              except:
                  pass
              
              if errdescn:
                  self._error.msg = errdescn

              if self._error.msg:
                  extrainfo(self._error.msg, '=> ',urltofetch)
              else:
                  extrainfo('URLError:',urltofetch)

              errnum = self._error.number

              # URL error basically wraps up socket error numbers
              # Why did I decide 10049 etc stand for Proxy server
              # error ? Need to check this...
              if errnum == 10049 or errnum == 10061: # Proxy server error
                  self._proxy_query(1, 1)

          except IOError, e:
              self._error.number = URL_IO_ERROR
              self._error.fatal=True
              self._error.errclass = "IOError"                                    
              self._error.msg = str(e)                    
              # Generated by invalid ftp hosts and
              # other reasons,
              # bug(url: http://www.gnu.org/software/emacs/emacs-paper.html)
              extrainfo(e,'=>',urltofetch)

          except BadStatusLine, e:
              self._error.number = URL_BADSTATUSLINE
              self._error.msg = str(e)
              self._error.errclass = "BadStatusLine"                                    
              extrainfo(e, '=> ',urltofetch)

          except TypeError, e:
              self._error.number = URL_TYPE_ERROR
              self._error.msg = str(e)
              self._error.errclass = "TypeError"                                    
              extrainfo(e, '=> ',urltofetch)
              
          except ValueError, e:
              self._error.number = URL_VALUE_ERROR
              self._error.msg = str(e)
              self._error.errclass = "ValueError"                                    
              extrainfo(e, '=> ',urltofetch)

          except AssertionError, e:
              self._error.number = URL_ASSERTION_ERROR
              self._error.msg = str(e)
              self._error.errclass = "AssertionError"                                    
              extrainfo(e ,'=> ',urltofetch)

          except socket.error, e:
              self._error.number = URL_SOCKET_ERROR                
              self._error.msg = str(e)
              self._error.errclass = "SocketError"                                    
              errmsg = self._error.msg

              extrainfo('Socket Error: ',errmsg,'=> ',urltofetch)

          except HarvestManFileObjectException, e:
              self._error.number = FILEOBJECT_EXCEPTION
              self._error.msg = str(e)
              self._error.errclass = "HarvestManFileObjectException"                                    
              errmsg = self._error.msg

              extrainfo('HarvestManFileObjectException: ',errmsg,'=> ',urltofetch)
              
          # attempt reconnect after some time
          # self.evnt.sleep()
          time.sleep(self._sleeptime)

      if self._data or self._datalen:
          return CONNECT_YES_DOWNLOADED
      else:
          return CONNECT_NO_ERROR

if __name__ == "__main__":

  test_url = 'http://hirise-pds.lpl.arizona.edu/PDS/RDR/PSP/ORB_001600_001699/PSP_001612_1780/PSP_001612_1780_RED.LBL'

  crawler = PdsCrawler('test.log')
  crawler.configure()
  result = crawler.get_url_data(test_url)
  crawler.finish()

  # we should be done here!
  print result

  sys.exit()