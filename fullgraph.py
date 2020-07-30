import sys, getopt
from ngMeta.MetaClient import MetaClient
from ngStorage.StorageClient import StorageClient
from ngStorage.ngProcessor.ScanEdgeProcessor import ScanEdgeProcessor
from ngStorage.ngProcessor.ScanVertexProcessor import ScanVertexProcessor

import networkx as nx

def scanEdge(space, returnCols, allCols):
    scanEdgeResponseIter = storageClient.scanEdge(space, returnCols, allCols, 100, 0, sys.maxsize)
    scanEdgeResponse = scanEdgeResponseIter.next()
    if scanEdgeResponse is None:
        print('fuck')
    else:
        print('ok')
    processEdge(space, scanEdgeResponse)
    while scanEdgeResponseIter.hasNext():
        scanEdgeResponse = scanEdgeResponseIter.next()
        if scanEdgeResponse is None:
            print("Error occurs while scaning edge")
            break
        processEdge(space, scanEdgeResponse)

def scanVertex(space, returnCols, allCols):
    scanVertexResponseIter = storageClient.scanVertex(space, returnCols, allCols, 100, 0, sys.maxsize)
    scanVertexResponse = scanVertexResponseIter.next()
    if scanVertexResponse is None:
        print('fuck vertex')
    else:
        print('ok vertex')
    processVertex(space, scanVertexResponse)
    while scanVertexResponseIter.hasNext():
        scanVertexResponse = scanVertexResponseIter.next()
        if scanVertexResponse is None:
            print("Error occurs while scaning vertex")
            break
        processVertex(space, scanVertexResponse)

def processEdge(space, scanEdgeResponse):
    result = scanEdgeProcessor.process(space, scanEdgeResponse)
    # Get the corresponding rows by edgeName
    for edgeName, edgeRows in result.rows.items():
        print('edgeName: ', edgeName)
        for row in edgeRows:
            srcId = row.defaultProperties[0].getValue()
            dstId = row.defaultProperties[2].getValue()
            print('srcId: ', srcId, ' dstId: ', dstId)
            props = {}
            for prop in row.properties:
                propName = prop.getName()
                propValue = prop.getValue()
                print('propName: ', propName, 'propValue: ', propValue)
                props[propName] = propValue
            G.add_edges_from([(srcId, dstId, props)])
            #print('-----------edge------------')
            #for defaultPro in row.defaultProperties:
            #    print(defaultPro.value)
            #print('*****************')
            #for pro in row.properties:
            #    print(pro.value)

def processVertex(space, scanVertexResponse):
    result = scanVertexProcessor.process(space, scanVertexResponse)
    for tagName, tagRows in result.rows.items():
        print('tagName: ', tagName)
        for row in tagRows:
            vid = row.defaultProperties[0].getValue()
            print('vid: ', vid)
            props = {}
            for prop in row.properties:
                propName = prop.getName()
                propValue = prop.getValue()
                print('propName: ', propName, ' propValue: ', propValue)
                props[propName] = propValue
            G.add_nodes_from([(vid, props)])
            #print('-----------vertex------------')
            #for defaultPro in row.defaultProperties:
            #    print(defaultPro.value)
            #print('*****************')
            #for pro in row.properties:
            #    print(pro.value)


if __name__ == '__main__':
    metaClient = MetaClient([(sys.argv[1], sys.argv[2])])
    metaClient.connect()
    storageClient = StorageClient(metaClient)
    scanEdgeProcessor = ScanEdgeProcessor(metaClient)
    scanVertexProcessor = ScanVertexProcessor(metaClient)

    myspace = 'my'
    returnCols = {}
    #returnCols['serve'] =  ['start_year', 'end_year']
    #returnCols['follow'] = ['degree']
    returnCols['book'] = ['name', 'degree', 'likeness', 'own']
    vertexReturnCols = {}
    vertexReturnCols['person'] = ['name', 'age', 'married', 'money']
    allCols = False
    
    G = nx.Graph()
    for space in metaClient.getPartsAllocFromCache().keys():
        if space == myspace:
            print('scaning space %s' % space)
            scanVertex(space, vertexReturnCols, allCols)
            scanEdge(space, returnCols, allCols)
    print(list(G.nodes))
    print(list(G.edges))
    print(list(nx.connected_components(G)))

