from meta.MetaService import Client
from ngData.data import Row
from ngData.data import RowReader
from ngData.data import Result
from storage.ttypes import ScanEdgeResponse

class ScanEdgeProcessor:
    def __init__(self, metaClient):
        self.metaClient = metaClient

    def process(self, spaceName, scanEdgeResponse):
        rowReaders = {}
        rows = {}
        edgeTypeNameMap = {}

        if scanEdgeResponse.edge_schema is not None:
            print('start to process edge_schema')
            for edgeType, schema in scanEdgeResponse.edge_schema.items():
                print('edgeType: ', edgeType)
                print('schema: ', schema)
                edgeName = self.metaClient.getEdgeNameFromCache(spaceName, edgeType)
                print('edgeName: ', edgeName)
                edgeItem = self.metaClient.getEdgeItemFromCache(spaceName, edgeName)
                print('edgeItem: ', edgeItem)
                schemaVersion = edgeItem.version
                print('schemaVersion: ', schemaVersion)
                rowReaders[edgeType] = RowReader(schema, schemaVersion)
                rows[edgeName] = [] ###
                edgeTypeNameMap[edgeType] = edgeName
        else:
            print('scanEdgeResponse.edge_schema is None')

        if scanEdgeResponse.edge_data is not None:
            print('start to process edge_data')
            for scanEdge in scanEdgeResponse.edge_data:
                edgeType = scanEdge.type
                if edgeType not in rowReaders.keys():
                    continue

                rowReader = rowReaders[edgeType]
                defaultProperties = rowReader.edgeKey(scanEdge.src, scanEdge.type, scanEdge.dst)
                properties = rowReader.decodeValue(scanEdge.value)
                edgeName = edgeTypeNameMap[edgeType]
                rows[edgeName].append(Row(defaultProperties, properties))
        else:
            print('scanEdgeResponse.edge_data is None')

        return Result(rows)
