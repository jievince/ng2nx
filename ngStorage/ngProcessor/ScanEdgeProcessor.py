from meta.MetaService import Client
from ngData.data import Row
from ngData.data import RowReader
from storage.ttypes import ScanEdgeResponse

class ScanEdgeProcessor:
    def __init__(self, metaClient):
        self.metaClient = metaClient

    def process(self, spaceName, scanEdgeResponse):
        rowReaders = {}
        rows = {}
        edgeTypeNameMap = {}

        if scanEdgeResponse.edge_schema is not None:
            for edgeType, schema in scanEdgeResponse.edge_schema.items():
                edgeName = metaClient.getEdgeNameFromCache(spaceName, edgeType)
                edgeItem = metaClient.getEdgeItemFromCache(spaceName, edgeName)
                schemaVersion = edgItem.version
                rowReaders[edgeType] = RowReader(schema, schemaVersion)
                rows[edgeName] = [] ###
                edgeTypeNameMap[edgeType] = edgeName

        if scanEdgeResponse.edge_data is not None:
            for scanEdge in scanEdgeResponse.edge_data:
                edgeType = scanEdge.type
                if edgeType not in rowReaders.keys():
                    continue

                rowReader = rowReaders[edgeType]
                defaultProperties = rowReader.edgeKey(scanEdge.src, scanEdge.type, scanEdge.dst)
                properties = rowReader.decodeValue(scanEdge.value)
                edgeName = edgeTypeNameMap[edgeType]
                rows[edgeName].append(Row(defaultProperties, properties))

        return Result(rows)
