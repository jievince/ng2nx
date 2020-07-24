import sys, getopt
from ngMeta.MetaClient import MetaClient
from ngStorage.StorageClient import StorageClient
from ngStorage.ngProcessor.ScanEdgeProcessor import ScanEdgeProcessor


def scanEdge(space, returnCols, allCols):
    scanEdgeResponseIter = storageClient.scanEdge(space, returnCols, allCols, 100, 0, sys.maxint)
    scanEdgeResponse = scanEdgeResponseIter.next()
    process(space, scanEdgeResponse)
    while scanEdgeResponseIter.hasNext():
        scanEdgeResponse = scanEdgeResponseIter.next()
        if scanEdgeResponse is None:
            # LOGGER.error("Error occurs while scaning edge")
            break
        process(space, scanEdgeResponse)

def process(space, scanEdgeResponse):
    result = scanEdgeProcessor.process(space, scanEdgeResponse)
    # Get the corresponding rows by edgeName
    edgeRows = result.rows['select']
    for row in edgeRows:
        print(row.defaultProperties)
        print(row.properties)

if __name__ == '__main__':
    metaClient = MetaClient([(sys.argv[1], sys.argv[2])])
    metaClient.connect()
    storageClient = StorageClient(metaClient)
    scanEdgeProcessor = ScanEdgeProcessor(metaClient)

    propNames = []
    propNames.append('grade')
    returnCols = {}
    returnCols['select'] = propNames
    allCols = False

    for space in metaClient.getPartsAllocFromCache().keys():
        scanEdge(space, returnCols, allCols)
