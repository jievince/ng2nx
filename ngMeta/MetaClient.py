from meta.MetaService import Client
from meta.ttypes import EdgeItem
from meta.ttypes import ErrorCode
from meta.ttypes import GetEdgeReq
from meta.ttypes import GetEdgeResp
from meta.ttypes import GetPartsAllocReq
from meta.ttypes import GetPartsAllocResp
from meta.ttypes import GetTagReq
from meta.ttypes import GetTagResp
from meta.ttypes import ListEdgesReq
from meta.ttypes import ListEdgesResp
from meta.ttypes import ListSpacesReq
from meta.ttypes import ListSpacesResp
from meta.ttypes import ListTagsReq
from meta.ttypes import ListTagsResp
from meta.ttypes import TagItem
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TCompactProtocol
from thrift.protocol import THeaderProtocol
import random

class MetaClient:
    def __init__(self, addresses, timeout=1000,
                connectionRetry=3, executionRetry=3):
        self.addresses = addresses
        self.timeout = timeout
        self.connectionRetry = connectionRetry
        self.executionRetry = executionRetry
        self.spaceNameMap = {} # map<spaceName, spaceId>
        self.spacePartLocation = {} # map<spaceName, map<partId, list<address>>>
        self.spaceTagItems = {} # map<spaceName, map<TagItem.tag_name, TagItem>>
        self.spaceEdgeItems = {} # map<spaceName, map<edgeItem.edge_name, edgeItem>
        self.tagNameMap = {} # map<spaceName, map<TagItem.tag_id, TagItem.tag_name>
        self.edgeNameMap = {}
        self.metaClient = None

    def connect(self):
        while self.connectionRetry > 0:
            code = self.doConnect(self.addresses)
            if code == 0:
                return ErrorCode.SUCCEEDED
            self.connectionRetry -= 1
        return ErrorCode.E_FAIL_TO_CONNECT

    def doConnect(self, addresses):
        address = addresses[random.randint(0, len(addresses)-1)]
        tTransport = TSocket.TSocket(address[0], address[1])
        tTransport.setTimeout(self.timeout)
        tTransport.open()
        tProtocol = TCompactProtocol(tTransport)
        metaClient = MetaClient(tProtocol)

        for spaceIdName in listSpaces():
            spaceName = spaceIdName.name # class IdName
            spaceNameMap[spaceName] = spaceIdName.id
            spacePartLocation[spaceName] = getPartsAlloc(spaceName)

            # Loading tag schema's cache
            tags = {}
            tagsName = {}
            for tagItem in getTags(spaceName):
                tags[tagItem.tag_name] = tagItem
                tagsName[tagItem.tag_id] = tagItem.tag_name

            spaceTagItems[spaceName] = tags
            tagNameMap[spaceName] = tagsName

            # Loading edge schema's cache
            edges = {}
            edgesName = {}
            for edgeItem in getEdges(spaceName):
                edges[edgeItem.edge_name] = edgeItem
                edgesName[edgeItem.edge_type] = edgeItem.edge_name
            spaceEdgeItems[spaceName] = edges
            edgeNameMap[spaceName] = edgesName

        return 0

    def getSpaceIdFromCache(self, spaceName):
        if spaceName not in spaceNameMap.keys():
            return -1
        else:
            return spaceNameMap[spaceName]
    
    def listSpaces(self):
        listSpacesReq = ListSpacesReq()
        listSpacesResp = metaClient.listSpaces(listSpacesReq)

        if listSpacesResp.code == ErrorCode.SUCCEEDED:
            return listSpacesResp.spaces()########## spaceNameID--> IdName
        else:
            # LOGGER.error("List Spaces Error Code: code")
            return None

    def getPartFromCache(self, spaceName, part):
        if spaceName not in spacePartLocation.keys():
            # 多线程时加锁
            spacePartLocation[spaceName] = getPartsAlloc(spaceName)

        partsAlloc = spacePartLocation[spaceName]
        if partsAlloc is None or part not in partsAlloc.keys():
            return None
        return partsAlloc[part]

    def getPartsAlloc(self, spaceName):
        spaceId = getSpaceIdFromCache(spaceName)
        # 检查 spaceId == -1 ???
        getPartsAllocReq = GetPartsAllocReq(spaceId)
        getPartsAllocResp = metaClient.getPartsAlloc(getPartsAllocReq)

        if getPartsAllocResp.code == ErrorCode.SUCCEEDED:
            addressMap = {}
            for partId, addresses in getPartsAllocResp.parts.items():
                # host ip, 转换
                addressMap[partId] = addresses

            return addressMap
        else:
            # LOGGER.error("Get Parts Error: code")
            return None

    def getPartsAllocFromCache(self):
        return spacePartLocation

    def getPartAllocFromCache(self, spaceName, part):
        if spaceName in spacePartLocation.keys():
            partsAlloc = spacePartLocation[spaceName]
            if part in partsAlloc:
                return partsAlloc[part]
        
        return None
    
    def getTagItemFromCache(self, spaceName, tagName):
        if spaceName not in spaceTagItems.keys():
            # 加锁
            tags = {}
            for tagItem in getTags(spaceName):
                tags[tagItem.tag_name] = tagItem
            spaceTagItems[spaceName] = tags

        tagItems = spaceTagItems[spaceName]
        if tagName in tagItems.keys(): # better way??? if tagName in tagItems.keys(): 
            return tagItems[tagName]
        return None

    def getTagNameFromCache(self, spaceName, tagId):
        if spaceName in tagNameMap.keys():
            tagNames = tagNameMap[spaceName]
            if tagId in tagNames.keys():
                return tagNames[tagId]

        return None

    def getTags(self, spaceName):
        spaceId = getSpaceIdFromCache(spaceName)
        # 需要检查spaceId吧？
        # if spaceId == -1 return None
        listTagsReq = ListTagsReq(spaceId)
        listTagsResp = metaClient.listTags(listTagsReq)
        
        if listTagsResp.code == ErrorCode.SUCCEEED:
            return listTagsResp.tags
        else:
            # LOGGER.error("Get Tags Error: code")
            return None

    def getTag(self, spaceName, tagName):
        spaceId = getSpaceIdFromCache(spaceName)
        getTagReq = GetTagReq(spaceId, tagName, -1)
        getTagResp = metaClient.getTag(getTagReq)

        if getTagResp.code == ErrorCode.SUCCEEDED:
            return getTagResp.schema
        else:
            return None

    def getTagSchema(self, spaceName, tagName, version=-1):
        spaceId = getSpaceIdFromCache(spaceName)
        getTagReq = GetTagReq(spaceId, tagName, version)
        getTagResp = metaClient.getTag(getTagReq)
        result = {}
        for columnDef in getTagResp.schema.columns:
            result[columDef.name] = columnDef.type.type # 需要将type convert to Class

        return result


    def getEdgeItemFromCache(self, spaceName, edgeName):
        if spaceName not in spaceEdgeItems.keys():
            # 加锁
            edges = {}
            for edgeItem in getEdges(spaceName):
                edges[edgeItem.edge_name] = edgeItem
            spaceEdgeItems[spaceName] = edges

        edgeItems = spaceEdgeItems[spaceName]
        if edgeName in edgeItems.keys():
            return edgeItems[edgeName]
        else:
            return None
    
    def getEdgeNameFromCache(self, spaceName, edgeType):
        if spaceName in edgeNameMap.keys():
            edgeNames = edgeNameMap[spaceName]
            if edgeType in edgeNames.keys():
                return edgeNames[edgeType]

        return None

    def getEdges(self, spaceName):
        spaceId = getSpaceIdFromCache(spaceName)
        listEdgesReq = ListEdgesReq(spaceId)
        listEdgesResp = metaClient.listEdges(listEdgesReq)
        if listEdgesResp.code == ErrorCode.SUCCEEDED:
            return listEdgeResp.edges
        else:
            # LOGGER.error("Get Tags Error: code")
            return None

    def getEdge(self, spaceName, edgeName):
        spaceId = getSpaceIdFromCache(spaceName)
        getEdgeReq = GetEdgeReq(spaceId, edgeName, -1)
        getEdgeResp = metaClient.getEdge(getEdgeReq)
        if getEdgeResp.code == ErrorCode.SUCCEEDED:
            return getEdgeResp.Schema
        else:
            return None

    def getEdgeSchema(self, spaceName, edgeName, version=-1):
        if spaceName not in spaceNameMap.keys():
            return None
        spaceId = spaceNameMap[spaceName]
        getEdgeReq = GetEdgeReq(spaceId, edgeName, version)
        getEdgeResp = metaClient.getEdge(getEdgeReq)
        for columnDef in getEdgeResp.schema.columns:
            result[columnDef.name] = columnDef.type.type # 需要converte it to Class

        return result
