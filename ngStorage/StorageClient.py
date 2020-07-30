from storage.StorageService import Client
from storage.ttypes import EntryId
from storage.ttypes import PropDef
from storage.ttypes import PropOwner
from storage.ttypes import ScanEdgeRequest
from storage.ttypes import ScanVertexRequest
from storage.ttypes import ErrorCode
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
import socket
import struct
import random

class Iterator:
  def __init__(self, it=None):
    self.it = iter(it)
    self._hasnext = None

  def __iter__(self): 
      return self

  def next(self):
    if self._hasnext:
      result = self._thenext
    else:
      result = next(self.it)
    self._hasnext = None
    return result

  def hasNext(self):
    if self._hasnext is None:
      try: self._thenext = next(self.it)
      except StopIteration: self._hasnext = False
      else: self._hasnext = True
    return self._hasnext


class ScanEdgeResponseIter:
    def __init__(self, clientDad, space, leader, scanEdgeRequest, storageClient):
        self.clientDad = clientDad
        self.space = space
        self.leader = leader
        self.scanEdgeRequest = scanEdgeRequest
        self.storageClient = storageClient
        self.cursor = None
        self.haveNext = True

    def hasNext(self):
        return self.haveNext

    def next(self):
        self.scanEdgeRequest.cursor = self.cursor
        print('scanEdgeRequest: ', self.scanEdgeRequest)
        scanEdgeResponse = self.storageClient.scanEdge(self.scanEdgeRequest)
        assert(scanEdgeResponse is not None), 'scanEdgeReponse is none'
        print('scanEdgeResponse: ', scanEdgeResponse)
        self.cursor = scanEdgeResponse.next_cursor
        self.haveNext = scanEdgeResponse.has_next

        if not isSuccessfully(scanEdgeResponse):
            print('scanEdgeResponse is not successfully, failed_codes: ', scanEdgeResponse.result.failed_codes)
            self.clientDad.handleResultCodes(scanEdgeResponse.result.failed_codes, self.space, self.storageClient, self.leader)
            self.haveNext = False
            return None
        else:
            return scanEdgeResponse
        
        return None


class ScanVertexResponseIter:
    def __init__(self, clientDad, space, leader, scanVertexRequest, storageClient):
        self.clientDad = clientDad
        self.space = space
        self.leader = leader
        self.scanVertexRequest = scanVertexRequest
        self.storageClient = storageClient
        self.cursor = None
        self.haveNext = True

    def hasNext(self):
        return self.haveNext

    def next(self):
        self.scanVertexRequest.cursor = self.cursor
        print('scanVertexRequest: ', self.scanVertexRequest)
        scanVertexResponse = self.storageClient.scanVertex(self.scanVertexRequest)
        assert(scanVertexResponse is not None), 'scanVertexReponse is none'
        print('scanVertexResponse: ', scanVertexResponse)
        self.cursor = scanVertexResponse.next_cursor
        self.haveNext = scanVertexResponse.has_next

        if not isSuccessfully(scanVertexResponse):
            print('scanVertexResponse is not successfully, failed_codes: ', scanVertexResponse.result.failed_codes)
            self.clientDad.handleResultCodes(scanVertexResponse.result.failed_codes, self.space, self.storageClient, self.leader)
            self.haveNext = False
            return None
        else:
            return scanVertexResponse

        return None


class ScanSpaceEdgeResponseIter:
    def __init__(self, clientDad, space, partIdsIter, returnCols, allCols, limit, startTime, endTime):
        self.clientDad = clientDad
        self.scanEdgeResponseIter = None
        self.space = space
        self.partIdsIter = partIdsIter
        self.returnCols = returnCols
        self.allCols = allCols
        self.limit = limit
        self.startTime = startTime
        self.endTime = endTime

    def hasNext(self):
        return self.partIdsIter.hasNext() or self.scanEdgeResponseIter.hasNext()

    def next(self):
        if self.scanEdgeResponseIter is None or not self.scanEdgeResponseIter.hasNext():
            part = self.partIdsIter.next() # 判断part.hasNext()??? is None?
            print('current part:', part)
            leader = self.clientDad.getLeader(self.space, part)
            if leader is None:
                #exception: part not found in space
                raise Exception('part %s not found in space %s' % (part, self.space))
            print('leader', leader)
            spaceId = self.clientDad.metaClient.getSpaceIdFromCache(self.space)
            if spaceId == -1:
                #exception: space not found
                raise Exception('space %s not found' % self.space)
            print('spaceId: ', spaceId)
            print('original returnCols: ', self.returnCols)
            colums = self.clientDad.getEdgeReturnCols(self.space, self.returnCols)
            scanEdgeRequest = ScanEdgeRequest(spaceId, part, None, colums, self.allCols, self.limit, self.startTime, self.endTime)
            self.scanEdgeResponseIter = self.clientDad.doScanEdge(self.space, leader, scanEdgeRequest)
            assert(self.scanEdgeResponseIter is not None), 'scanEdgeResponseIter is None'
        
        return self.scanEdgeResponseIter.next()


class ScanSpaceVertexResponseIter:
    def __init__(self, clientDad, space, partIdsIter, returnCols, allCols, limit, startTime, endTime):
        self.clientDad = clientDad
        self.scanVertexResponseIter = None
        self.space = space
        self.partIdsIter = partIdsIter
        self.returnCols = returnCols
        self.allCols = allCols
        self.limit = limit
        self.startTime = startTime
        self.endTime = endTime

    def hasNext(self):
        return self.partIdsIter.hasNext() or self.scanVertexResponseIter.hasNext()

    def next(self):
        if self.scanVertexResponseIter is None or not self.scanVertexResponseIter.hasNext():
            part = self.partIdsIter.next() # 判断part.hasNext()??? is None?
            print('current part:', part)
            leader = self.clientDad.getLeader(self.space, part)
            if leader is None:
                #exception: part not found in space
                raise Exception('part %s not found in space %s' % (part, self.space))
            print('leader', leader)
            spaceId = self.clientDad.metaClient.getSpaceIdFromCache(self.space)
            if spaceId == -1:
                #exception: space not found
                raise Exception('space %s not found' % self.space)
            print('spaceId: ', spaceId)
            print('original returnCols: ', self.returnCols)
            colums = self.clientDad.getVertexReturnCols(self.space, self.returnCols)
            scanVertexRequest = ScanVertexRequest(spaceId, part, None, colums, self.allCols, self.limit, self.startTime, self.endTime)
            self.scanVertexResponseIter = self.clientDad.doScanVertex(self.space, leader, scanVertexRequest)
            assert(self.scanVertexResponseIter is not None), 'scanVertexResponseIter is None'

        return self.scanVertexResponseIter.next()


class StorageClient:
    def __init__(self, metaClient):
        self.metaClient = metaClient
        self.partsAlloc = metaClient.getPartsAllocFromCache()
        self.storageClients = {}
        self.leaders = {}
        self.timeout = 1000

    def connect(self, address):
        if address not in self.storageClients.keys():
            client = self.doConnect(address)
            self.storageClients[address] = client
            return client
        else:
            return self.storageClients[address]

    def disConnect(self, address):
        self.storageClients.remove(address)

    def doConnects(self, addresses):
        for address in addresses:
            client = self.doConnect(address)
            self.storageClients[address] = client

    def doConnect(self, address):
        ip = socket.inet_ntoa(struct.pack('I',socket.htonl(address.ip)))
        port = address.port
        print('StorageClient is connect to: tTransport ip: %s, port: %s' % (ip, port))
        tTransport = TSocket.TSocket(ip, port)
        if self.timeout > 0:
            tTransport.setTimeout(self.timeout)
            tTransport = TTransport.TBufferedTransport(tTransport)
            tProtocol = TBinaryProtocol.TBinaryProtocol(tTransport)
            tTransport.open()
        return Client(tProtocol)

    def scanEdge(self, space, returnCols, allCols, limit, startTime, endTime):
        partIds = self.metaClient.getPartsAllocFromCache()[space].keys()
        for part in partIds:
            print('partId: %s' % part)
        partIdsIter = Iterator(partIds)
        return ScanSpaceEdgeResponseIter(self, space, partIdsIter, returnCols, allCols, limit, startTime, endTime)
    
    def scanVertex(self, space, returnCols, allCols, limit, startTime, endTime):
        partIds = self.metaClient.getPartsAllocFromCache()[space].keys()
        for part in partIds:
            print('partId: %s' % part)
        partIdsIter = Iterator(partIds)
        return ScanSpaceVertexResponseIter(self, space, partIdsIter, returnCols, allCols, limit, startTime, endTime)

    def doScanEdge(self, space, leader, scanEdgeRequest):
        client  = self.connect(leader)
        if client is None:
            print('cannot connect to leader:', leader)
            self.disConnect(leader)
            return None

        return ScanEdgeResponseIter(self, space, leader, scanEdgeRequest, client)
    
    def doScanVertex(self, space, leader, scanVertexRequest):
        client = self.connect(leader)
        if client is None:
            print('cannot connect to leader:', leader)
            self.disConnect(leader)
            return None

        return ScanVertexResponseIter(self, space, leader, scanVertexRequest, client)

    def getEdgeReturnCols(self, space, returnCols):
        columns = {}
        for edgeName, propNames in returnCols.items():
            edgeItem = self.metaClient.getEdgeItemFromCache(space, edgeName)
            if edgeItem is None:
                # exception: edge not found in space
                raise Exception('edge %s not found in space %s' % (edgeName, space))
            edgeType = edgeItem.edge_type
            entryId = EntryId(edge_type=edgeType)
            propDefs = []
            for propName in propNames:
                propDef = PropDef(PropOwner.EDGE, entryId, propName)
                propDefs.append(propDef)
            columns[edgeType] = propDefs
        return columns
    
    def getVertexReturnCols(self, space, returnCols):
        columns = {}
        for tagName, propNames in returnCols.items():
            tagItem = self.metaClient.getTagItemFromCache(space, tagName)
            if tagItem is None:
                raise Exception('tag %s not found in space %s' % (tagName, space))
            tagId = tagItem.tag_id
            entryId = EntryId(tag_id=tagId)
            propDefs = []
            for propName in propNames:
                propDef = PropDef(PropOwner.SOURCE, entryId, propName)
                propDefs.append(propDef)
            columns[tagId] = propDefs
        return columns

    def getLeader(self, spaceName, part):
        if spaceName not in self.leaders.keys():
            self.leaders[spaceName] = {}
    
        if part in self.leaders[spaceName].keys():
            return self.leaders[spaceName][part]
        else:
            addresses = self.metaClient.getPartsAllocFromCache()[spaceName][part]
            if addresses is None:
                return None
            leader = addresses[random.randint(0, len(addresses)-1)]
            self.leaders[spaceName][part] = leader
            return leader
    def handleResultCodes(self, failedCodes, space, client, leader):
        for resultCode in failedCodes:
            if resultCode.code == ErrorCode.E_LEADER_CHANGED:
                hostAddr = code.leader
                if hostAddr is not None and hostAddr.ip != 0 and hostAddr.port != 0:
                    host = socket.inet_ntoa(struct.pack('I',socket.htonl(hostAddr.ip)))
                    port = hostAddr.port
                    newLeader = (host, port)
                    self.updateLeader(space, code.part_id, newLeader)
                    newClient = self.storageClients[newLeader]
                    if newClient is not None:
                        client =  newClient
                        leader = newLeader

def isSuccessfully(response):
    return len(response.result.failed_codes) == 0
