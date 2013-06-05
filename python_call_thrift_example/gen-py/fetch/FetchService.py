#-*- coding: UTF-8 -*-
#
# Autogenerated by Thrift Compiler (0.9.0)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#
#  options string: py
#

from thrift.Thrift import TType, TMessageType, TException, TApplicationException
from ttypes import *
from thrift.Thrift import TProcessor
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TProtocol
try:
  from thrift.protocol import fastbinary
except:
  fastbinary = None


class Iface:
  def addFetchRecord(self, srcUrl, optFrom, comment, suffix):
    """
    增加抓取

    Parameters:
     - srcUrl
     - optFrom
     - comment
     - suffix
    """
    pass

  def getFetchRecordCount(self, optFrom):
    """
    获取抓取记录数量

    Parameters:
     - optFrom
    """
    pass

  def getUrlBySrcUrl(self, srcUrl):
    """
    根据srcUrl获取已经转换为本地的url

    Parameters:
     - srcUrl
    """
    pass

  def listFetchRecord(self, optFrom, offset, count):
    """
    列出抓取记录

    Parameters:
     - optFrom
     - offset
     - count
    """
    pass

  def deleteFetchRecord(self, srcUrl):
    """
    删除抓取记录

    Parameters:
     - srcUrl
    """
    pass


class Client(Iface):
  def __init__(self, iprot, oprot=None):
    self._iprot = self._oprot = iprot
    if oprot is not None:
      self._oprot = oprot
    self._seqid = 0

  def addFetchRecord(self, srcUrl, optFrom, comment, suffix):
    """
    增加抓取

    Parameters:
     - srcUrl
     - optFrom
     - comment
     - suffix
    """
    self.send_addFetchRecord(srcUrl, optFrom, comment, suffix)
    self.recv_addFetchRecord()

  def send_addFetchRecord(self, srcUrl, optFrom, comment, suffix):
    self._oprot.writeMessageBegin('addFetchRecord', TMessageType.CALL, self._seqid)
    args = addFetchRecord_args()
    args.srcUrl = srcUrl
    args.optFrom = optFrom
    args.comment = comment
    args.suffix = suffix
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_addFetchRecord(self, ):
    (fname, mtype, rseqid) = self._iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(self._iprot)
      self._iprot.readMessageEnd()
      raise x
    result = addFetchRecord_result()
    result.read(self._iprot)
    self._iprot.readMessageEnd()
    if result.e is not None:
      raise result.e
    return

  def getFetchRecordCount(self, optFrom):
    """
    获取抓取记录数量

    Parameters:
     - optFrom
    """
    self.send_getFetchRecordCount(optFrom)
    return self.recv_getFetchRecordCount()

  def send_getFetchRecordCount(self, optFrom):
    self._oprot.writeMessageBegin('getFetchRecordCount', TMessageType.CALL, self._seqid)
    args = getFetchRecordCount_args()
    args.optFrom = optFrom
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_getFetchRecordCount(self, ):
    (fname, mtype, rseqid) = self._iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(self._iprot)
      self._iprot.readMessageEnd()
      raise x
    result = getFetchRecordCount_result()
    result.read(self._iprot)
    self._iprot.readMessageEnd()
    if result.success is not None:
      return result.success
    if result.e is not None:
      raise result.e
    raise TApplicationException(TApplicationException.MISSING_RESULT, "getFetchRecordCount failed: unknown result");

  def getUrlBySrcUrl(self, srcUrl):
    """
    根据srcUrl获取已经转换为本地的url

    Parameters:
     - srcUrl
    """
    self.send_getUrlBySrcUrl(srcUrl)
    return self.recv_getUrlBySrcUrl()

  def send_getUrlBySrcUrl(self, srcUrl):
    self._oprot.writeMessageBegin('getUrlBySrcUrl', TMessageType.CALL, self._seqid)
    args = getUrlBySrcUrl_args()
    args.srcUrl = srcUrl
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_getUrlBySrcUrl(self, ):
    (fname, mtype, rseqid) = self._iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(self._iprot)
      self._iprot.readMessageEnd()
      raise x
    result = getUrlBySrcUrl_result()
    result.read(self._iprot)
    self._iprot.readMessageEnd()
    if result.success is not None:
      return result.success
    if result.e is not None:
      raise result.e
    raise TApplicationException(TApplicationException.MISSING_RESULT, "getUrlBySrcUrl failed: unknown result");

  def listFetchRecord(self, optFrom, offset, count):
    """
    列出抓取记录

    Parameters:
     - optFrom
     - offset
     - count
    """
    self.send_listFetchRecord(optFrom, offset, count)
    return self.recv_listFetchRecord()

  def send_listFetchRecord(self, optFrom, offset, count):
    self._oprot.writeMessageBegin('listFetchRecord', TMessageType.CALL, self._seqid)
    args = listFetchRecord_args()
    args.optFrom = optFrom
    args.offset = offset
    args.count = count
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_listFetchRecord(self, ):
    (fname, mtype, rseqid) = self._iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(self._iprot)
      self._iprot.readMessageEnd()
      raise x
    result = listFetchRecord_result()
    result.read(self._iprot)
    self._iprot.readMessageEnd()
    if result.success is not None:
      return result.success
    if result.e is not None:
      raise result.e
    raise TApplicationException(TApplicationException.MISSING_RESULT, "listFetchRecord failed: unknown result");

  def deleteFetchRecord(self, srcUrl):
    """
    删除抓取记录

    Parameters:
     - srcUrl
    """
    self.send_deleteFetchRecord(srcUrl)
    self.recv_deleteFetchRecord()

  def send_deleteFetchRecord(self, srcUrl):
    self._oprot.writeMessageBegin('deleteFetchRecord', TMessageType.CALL, self._seqid)
    args = deleteFetchRecord_args()
    args.srcUrl = srcUrl
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_deleteFetchRecord(self, ):
    (fname, mtype, rseqid) = self._iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(self._iprot)
      self._iprot.readMessageEnd()
      raise x
    result = deleteFetchRecord_result()
    result.read(self._iprot)
    self._iprot.readMessageEnd()
    if result.e is not None:
      raise result.e
    return


class Processor(Iface, TProcessor):
  def __init__(self, handler):
    self._handler = handler
    self._processMap = {}
    self._processMap["addFetchRecord"] = Processor.process_addFetchRecord
    self._processMap["getFetchRecordCount"] = Processor.process_getFetchRecordCount
    self._processMap["getUrlBySrcUrl"] = Processor.process_getUrlBySrcUrl
    self._processMap["listFetchRecord"] = Processor.process_listFetchRecord
    self._processMap["deleteFetchRecord"] = Processor.process_deleteFetchRecord

  def process(self, iprot, oprot):
    (name, type, seqid) = iprot.readMessageBegin()
    if name not in self._processMap:
      iprot.skip(TType.STRUCT)
      iprot.readMessageEnd()
      x = TApplicationException(TApplicationException.UNKNOWN_METHOD, 'Unknown function %s' % (name))
      oprot.writeMessageBegin(name, TMessageType.EXCEPTION, seqid)
      x.write(oprot)
      oprot.writeMessageEnd()
      oprot.trans.flush()
      return
    else:
      self._processMap[name](self, seqid, iprot, oprot)
    return True

  def process_addFetchRecord(self, seqid, iprot, oprot):
    args = addFetchRecord_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = addFetchRecord_result()
    try:
      self._handler.addFetchRecord(args.srcUrl, args.optFrom, args.comment, args.suffix)
    except error.ttypes.ServiceException as e:
      result.e = e
    oprot.writeMessageBegin("addFetchRecord", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

  def process_getFetchRecordCount(self, seqid, iprot, oprot):
    args = getFetchRecordCount_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = getFetchRecordCount_result()
    try:
      result.success = self._handler.getFetchRecordCount(args.optFrom)
    except error.ttypes.ServiceException as e:
      result.e = e
    oprot.writeMessageBegin("getFetchRecordCount", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

  def process_getUrlBySrcUrl(self, seqid, iprot, oprot):
    args = getUrlBySrcUrl_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = getUrlBySrcUrl_result()
    try:
      result.success = self._handler.getUrlBySrcUrl(args.srcUrl)
    except error.ttypes.ServiceException as e:
      result.e = e
    oprot.writeMessageBegin("getUrlBySrcUrl", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

  def process_listFetchRecord(self, seqid, iprot, oprot):
    args = listFetchRecord_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = listFetchRecord_result()
    try:
      result.success = self._handler.listFetchRecord(args.optFrom, args.offset, args.count)
    except error.ttypes.ServiceException as e:
      result.e = e
    oprot.writeMessageBegin("listFetchRecord", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

  def process_deleteFetchRecord(self, seqid, iprot, oprot):
    args = deleteFetchRecord_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = deleteFetchRecord_result()
    try:
      self._handler.deleteFetchRecord(args.srcUrl)
    except error.ttypes.ServiceException as e:
      result.e = e
    oprot.writeMessageBegin("deleteFetchRecord", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()


# HELPER FUNCTIONS AND STRUCTURES

class addFetchRecord_args:
  """
  Attributes:
   - srcUrl
   - optFrom
   - comment
   - suffix
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'srcUrl', None, None, ), # 1
    (2, TType.I32, 'optFrom', None, None, ), # 2
    (3, TType.STRING, 'comment', None, None, ), # 3
    (4, TType.STRING, 'suffix', None, None, ), # 4
  )

  def __init__(self, srcUrl=None, optFrom=None, comment=None, suffix=None,):
    self.srcUrl = srcUrl
    self.optFrom = optFrom
    self.comment = comment
    self.suffix = suffix

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.srcUrl = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.I32:
          self.optFrom = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.STRING:
          self.comment = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.STRING:
          self.suffix = iprot.readString();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('addFetchRecord_args')
    if self.srcUrl is not None:
      oprot.writeFieldBegin('srcUrl', TType.STRING, 1)
      oprot.writeString(self.srcUrl)
      oprot.writeFieldEnd()
    if self.optFrom is not None:
      oprot.writeFieldBegin('optFrom', TType.I32, 2)
      oprot.writeI32(self.optFrom)
      oprot.writeFieldEnd()
    if self.comment is not None:
      oprot.writeFieldBegin('comment', TType.STRING, 3)
      oprot.writeString(self.comment)
      oprot.writeFieldEnd()
    if self.suffix is not None:
      oprot.writeFieldBegin('suffix', TType.STRING, 4)
      oprot.writeString(self.suffix)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class addFetchRecord_result:
  """
  Attributes:
   - e
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRUCT, 'e', (error.ttypes.ServiceException, error.ttypes.ServiceException.thrift_spec), None, ), # 1
  )

  def __init__(self, e=None,):
    self.e = e

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRUCT:
          self.e = error.ttypes.ServiceException()
          self.e.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('addFetchRecord_result')
    if self.e is not None:
      oprot.writeFieldBegin('e', TType.STRUCT, 1)
      self.e.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class getFetchRecordCount_args:
  """
  Attributes:
   - optFrom
  """

  thrift_spec = (
    None, # 0
    (1, TType.I32, 'optFrom', None, None, ), # 1
  )

  def __init__(self, optFrom=None,):
    self.optFrom = optFrom

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.I32:
          self.optFrom = iprot.readI32();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('getFetchRecordCount_args')
    if self.optFrom is not None:
      oprot.writeFieldBegin('optFrom', TType.I32, 1)
      oprot.writeI32(self.optFrom)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class getFetchRecordCount_result:
  """
  Attributes:
   - success
   - e
  """

  thrift_spec = (
    (0, TType.I32, 'success', None, None, ), # 0
    (1, TType.STRUCT, 'e', (error.ttypes.ServiceException, error.ttypes.ServiceException.thrift_spec), None, ), # 1
  )

  def __init__(self, success=None, e=None,):
    self.success = success
    self.e = e

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 0:
        if ftype == TType.I32:
          self.success = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 1:
        if ftype == TType.STRUCT:
          self.e = error.ttypes.ServiceException()
          self.e.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('getFetchRecordCount_result')
    if self.success is not None:
      oprot.writeFieldBegin('success', TType.I32, 0)
      oprot.writeI32(self.success)
      oprot.writeFieldEnd()
    if self.e is not None:
      oprot.writeFieldBegin('e', TType.STRUCT, 1)
      self.e.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class getUrlBySrcUrl_args:
  """
  Attributes:
   - srcUrl
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'srcUrl', None, None, ), # 1
  )

  def __init__(self, srcUrl=None,):
    self.srcUrl = srcUrl

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.srcUrl = iprot.readString();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('getUrlBySrcUrl_args')
    if self.srcUrl is not None:
      oprot.writeFieldBegin('srcUrl', TType.STRING, 1)
      oprot.writeString(self.srcUrl)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class getUrlBySrcUrl_result:
  """
  Attributes:
   - success
   - e
  """

  thrift_spec = (
    (0, TType.STRING, 'success', None, None, ), # 0
    (1, TType.STRUCT, 'e', (error.ttypes.ServiceException, error.ttypes.ServiceException.thrift_spec), None, ), # 1
  )

  def __init__(self, success=None, e=None,):
    self.success = success
    self.e = e

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 0:
        if ftype == TType.STRING:
          self.success = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 1:
        if ftype == TType.STRUCT:
          self.e = error.ttypes.ServiceException()
          self.e.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('getUrlBySrcUrl_result')
    if self.success is not None:
      oprot.writeFieldBegin('success', TType.STRING, 0)
      oprot.writeString(self.success)
      oprot.writeFieldEnd()
    if self.e is not None:
      oprot.writeFieldBegin('e', TType.STRUCT, 1)
      self.e.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class listFetchRecord_args:
  """
  Attributes:
   - optFrom
   - offset
   - count
  """

  thrift_spec = (
    None, # 0
    (1, TType.I32, 'optFrom', None, None, ), # 1
    (2, TType.I32, 'offset', None, None, ), # 2
    (3, TType.I32, 'count', None, None, ), # 3
  )

  def __init__(self, optFrom=None, offset=None, count=None,):
    self.optFrom = optFrom
    self.offset = offset
    self.count = count

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.I32:
          self.optFrom = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.I32:
          self.offset = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.I32:
          self.count = iprot.readI32();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('listFetchRecord_args')
    if self.optFrom is not None:
      oprot.writeFieldBegin('optFrom', TType.I32, 1)
      oprot.writeI32(self.optFrom)
      oprot.writeFieldEnd()
    if self.offset is not None:
      oprot.writeFieldBegin('offset', TType.I32, 2)
      oprot.writeI32(self.offset)
      oprot.writeFieldEnd()
    if self.count is not None:
      oprot.writeFieldBegin('count', TType.I32, 3)
      oprot.writeI32(self.count)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class listFetchRecord_result:
  """
  Attributes:
   - success
   - e
  """

  thrift_spec = (
    (0, TType.LIST, 'success', (TType.STRUCT,(FetchRecord, FetchRecord.thrift_spec)), None, ), # 0
    (1, TType.STRUCT, 'e', (error.ttypes.ServiceException, error.ttypes.ServiceException.thrift_spec), None, ), # 1
  )

  def __init__(self, success=None, e=None,):
    self.success = success
    self.e = e

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 0:
        if ftype == TType.LIST:
          self.success = []
          (_etype3, _size0) = iprot.readListBegin()
          for _i4 in xrange(_size0):
            _elem5 = FetchRecord()
            _elem5.read(iprot)
            self.success.append(_elem5)
          iprot.readListEnd()
        else:
          iprot.skip(ftype)
      elif fid == 1:
        if ftype == TType.STRUCT:
          self.e = error.ttypes.ServiceException()
          self.e.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('listFetchRecord_result')
    if self.success is not None:
      oprot.writeFieldBegin('success', TType.LIST, 0)
      oprot.writeListBegin(TType.STRUCT, len(self.success))
      for iter6 in self.success:
        iter6.write(oprot)
      oprot.writeListEnd()
      oprot.writeFieldEnd()
    if self.e is not None:
      oprot.writeFieldBegin('e', TType.STRUCT, 1)
      self.e.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class deleteFetchRecord_args:
  """
  Attributes:
   - srcUrl
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'srcUrl', None, None, ), # 1
  )

  def __init__(self, srcUrl=None,):
    self.srcUrl = srcUrl

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.srcUrl = iprot.readString();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('deleteFetchRecord_args')
    if self.srcUrl is not None:
      oprot.writeFieldBegin('srcUrl', TType.STRING, 1)
      oprot.writeString(self.srcUrl)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class deleteFetchRecord_result:
  """
  Attributes:
   - e
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRUCT, 'e', (error.ttypes.ServiceException, error.ttypes.ServiceException.thrift_spec), None, ), # 1
  )

  def __init__(self, e=None,):
    self.e = e

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRUCT:
          self.e = error.ttypes.ServiceException()
          self.e.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('deleteFetchRecord_result')
    if self.e is not None:
      oprot.writeFieldBegin('e', TType.STRUCT, 1)
      self.e.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)
