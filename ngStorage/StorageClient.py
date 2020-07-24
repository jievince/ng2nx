from storage.StorageService import Client
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TCompactProtocol
from thrift.protocol import THeaderProtocol

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
    def __init__(self, space, leader, scanEdgeRequest, storageClient):
        self.space = space
        self.leader = leader
        self.scanEdgeRequest = scanEdgeReqeust
        self.storgeClient = storageClient
        self.cursor = None
        self.haveNext = True

    def hasNext(self):
        return haveNext

    def next(self):
        scanEdgeRequest.cursor = cursor
        scanEdgeResponse = storageClient.scanEdge(scanEdgeRequest)
        cursor = scanEdgeResponse.next_cursor
        haveNext = scanEdgeResponse.has_next

        if not isSuccessfully(scanEdgeResponse):
            handleResultCodes(scanEdgeResponse.result.failed_codes, space, client, leader)
            haveNext = False
            return None
        else:
            return scanEdgeResponse
        
        return None


class ScanSpaceEdgeResponseIter:
    def __init__(self, space, partIdsIter, returnCols, allCols, limit, startTime, endTime):
        self.scanEdgeResponseIter = None
        self.space = space
        self.partidsIter = partIdsIter
        self.returnCols = returnCols
        self.allCols = allCols
        self.limit = limit
        self.startTime = startTime
        self.endTime = endTime

    def hasNext(self):
        return scanEdgeResponseIter.hasNext()

    def next(self):
        if scanEdgeResponseIter is None or not scanEdgeResponseIter.hasNext():
            part = partIdsIter.next() # 判断part is None?
            leader = getLeader(space, part)
            if leader is None:
                #exception: part not found in space
                pass
            spaceId = getSpaceIdFromCache(space)
            if spaceId == -1:
                #exception: space not found
                pass

            colums = getEdgeReturnCols(space, returnCols)
            scanEdgeRequest = ScanEdgeRequest(spaceId, part, colums, allCols, limit, startTime, endTime)
            
            scanEdgeResponseIter = doScanEdge(space, leader, scanEdgeRequest)

            return scanEdgeResponseIter.next()


class StorageClient:
    def __init__(self, metaClient):
        self.metaClient = metaClient
        self.partsAlloc = metaClient.getPartsAllocFromCache()

    def doConnects(self, addresses):
        for address in addresses:
            client = doConnect(address)
            self.storageClients[address] = client

    def doConnect(self, address):
        tTransport = TSocket(address.host, address.port, timeout)
        tTransport.open()
        tProtocol = TCompactProtocol(tTransport)
        return Client(tProtocol)

    def put(self, spaceName, key, value):
        spaceId = metaClient.getSpaceIdFromCache(spaceName)
        part = keyToPartId(spaceName, key)
        leader = getLeader(spaceName, part)
        if leader == None:
            return False

        parts = {part:[(key, value)]}
        request = PutReuest(spaceId, parts)
        
        return doPut(spaceName, leader, request)

    def doPut(self, spaceName, leader, request):
        client = connect(leader) # connect exception, return None
        if client == None:
            disconnect(leader)
            return False

        execResponse = client.put(request)
        if not isSuccessfully(execResponse):
            handleResultCodes(execResponse.result.failed_codes, space, client, leader)
        else:
            return True

    def get(self, space, key):
        spaceId = metaClient.getSpaceIdFromeCache(spaceName)
        part = keyToPart(spaceName, key)
        leader = getLeader(spaceName, part)
        if leader == None:
            return False
        
        parts = {part:[key]}
        request = GetRequest(spaceId, parts)
        result = doGet(spaceName, leader, request) # map<key, value>
        if result == None or key not in result.keys():
            return None
        else:
            return result[key]

    def doGet(self, space, leader, request):
        client = connect(leader)
        if client == None:
            disconnect(leader)
            return False

        generalResponse = client.get(request)
        if not isSuccessfully(generalResponse):
            handleResultCodes(generalResponse.result.failed_codes, space, client, leader)
        else:
            return generalResponse.values
    
    def scanEdge(self, space, returnCols, allCols, limit, startTime, endTime):
        partIds = metaClient.getPartsAllocFromCache()[space].keys()
        partIdsIter = Iterator(partIds)
        return ScanSpaceEdge(space, partIdsIter, returnCols, allCols, limit, startTime, endTime)

    def scanSpaceEdge(self, space, partIdsIter, returnCols, allCols, limit, startTime, endTime):
        return ScanSpaceEdgeResponseIter(space, partIdsIter, returnCols, allCols, limit, startTime, endTime)
        
    def doScanEdge(self, space, leader, scanEdgeRequest):
        storageClient = connect(leader)
        if client is None:
            client.disConnect()
            return None

        return ScanEdgeResponseIter(space, leader, scanEdgeRequest, storageClient)

    def scanVertex(self, space, returnCols ):
        pass

def getEdgeReturnCols(space, returnCols):
    columns = {}
    for edgeName, propNames in returnCols:
        edgeItem = getEdgeItemFromCache(space, edgeName)
        if edgeItme is None:
            # exception: edge not found in space
            pass
        edgeType = edgeItem.edge_type
        entryId = EntryId(edge_type=edgeType)
        propDefs = []
        for propName in propNames:
            propDef = PropDef(PropOwner.EDGE)
            propDefs.append(propDef)
        columns[edgeType] = propDefs
    
    return columns


def isSuccessfully(execResponse):
    return execResponse.result.failed_codes.size() == 0

def getLeader(spaceName, part):
    if spaceName not in leaders.keys():
        leaders[spaceName] = {}
    
    if part in leaders[spaceName].keys():
        return leaders[spaceName][part]
    else:
        addresses = metaClient.getPartsAllocFromCache()[space][part]
        if addresses is None:
            return None
        leader = addresses[random.randint(0, len(addresses)-1)]
        leaders[space][part] = leader
        return leader

def connnect(self, address):
    if address not in storageClients.keys():
        client = doConnect(address)
        storageClients[address] = client
        return client
    else:
        return storageClients[address]

def disConnect(self, address):
    storageClients.remove(address)
