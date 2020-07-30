import sys, getopt
from ngMeta.MetaClient import MetaClient
from ngStorage.StorageClient import StorageClient
from ngStorage.ngProcessor.ScanEdgeProcessor import ScanEdgeProcessor
from ngStorage.ngProcessor.ScanVertexProcessor import ScanVertexProcessor

import networkx as nx
import matplotlib.pyplot as plt

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

def getReturnCols(space):
    tagItems = metaClient.getTags(space)
    vertexReturnCols = {}
    for tagItem in tagItems:
        tagName = tagItem.tag_name
        vertexReturnCols[tagName] = metaClient.getTagSchema(space, tagName).keys()
    edgeItems = metaClient.getEdges(space)
    edgeReturnCols = {}
    for edgeItem in edgeItems:
        edgeName = edgeItem.edge_name
        edgeReturnCols[edgeName] = metaClient.getEdgeSchema(space, edgeName).keys()

    return vertexReturnCols, edgeReturnCols


if __name__ == '__main__':
    metaClient = MetaClient([(sys.argv[1], sys.argv[2])])
    metaClient.connect()
    storageClient = StorageClient(metaClient)
    scanEdgeProcessor = ScanEdgeProcessor(metaClient)
    scanVertexProcessor = ScanVertexProcessor(metaClient)

    spaceToRead = sys.argv[3]
    #returnCols['serve'] =  ['start_year', 'end_year']
    #returnCols['follow'] = ['degree']
    #returnCols['book'] = ['name', 'degree', 'likeness', 'own']
    vertexReturnCols, edgeReturnCols = getReturnCols(spaceToRead)
    #vertexReturnCols['person'] = ['name', 'age', 'married', 'money']
    allCols = True
    
    G = nx.Graph()
    if spaceToRead not in metaClient.getPartsAllocFromCache().keys():
        raise Exception('spaceToRead %s is not found in nebula')
    else:
        print('scaning space %s' % spaceToRead)
        scanVertex(spaceToRead, vertexReturnCols, allCols)
        scanEdge(spaceToRead, edgeReturnCols, allCols)
    print('nodes: ', list(G.nodes))
    print('edges: ', list(G.edges))
    print('connected subgraph: ', list(nx.connected_components(G)))
    nx.draw(G, with_labels=True, font_weight='bold')
    plt.show()
    plt.savefig('./test.png')
