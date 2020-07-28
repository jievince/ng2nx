import sys, getopt
from ngMeta.MetaClient import MetaClient
from ngStorage.StorageClient import StorageClient
from ngStorage.ngProcessor.ScanEdgeProcessor import ScanEdgeProcessor


def scanEdge(space, returnCols, allCols):
    scanEdgeResponseIter = storageClient.scanEdge(space, returnCols, allCols, 100, 0, sys.maxsize)
    scanEdgeResponse = scanEdgeResponseIter.next()
    if scanEdgeResponse is None:
        print('fuck')
    else:
        print('ok')
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
    edgeRows = result.rows['serve']
    for row in edgeRows:
        print(row.defaultProperties)
        print(row.properties)

if __name__ == '__main__':
    metaClient = MetaClient([(sys.argv[1], sys.argv[2])])
    metaClient.connect()
    storageClient = StorageClient(metaClient)
    scanEdgeProcessor = ScanEdgeProcessor(metaClient)

    returnCols = {}
    returnCols['serve'] = ['start_year', 'end_year']
    returnCols['follow'] = ['degree']
    allCols = False

    for space in metaClient.getPartsAllocFromCache().keys():
        print('scaning space %s' % space)
        scanEdge(space, returnCols, allCols)
