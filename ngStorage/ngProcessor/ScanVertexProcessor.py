from meta.MetaService import Client
from ngData.data import Row
from ngData.data import RowReader
from ngData.data import Result
from storage.ttypes import ScanVertexResponse

class ScanVertexProcessor:
    def __init__(self, metaClient):
        self.metaClient = metaClient

    def process(self, spaceName, scanVertexResponse):
        if scanVertexResponse is None:
            print('process: scanVertexResponse is None')
            return None
        rowReaders = {}
        rows = {}
        tagIdNameMap = {}
        if scanVertexResponse.vertex_schema is not None:
            print('start to process vertex_schema')
            for tagId, schema in scanVertexResponse.vertex_schema.items():
                print('tagId: ', tagId)
                print('schema: ', schema)
                tagName = self.metaClient.getTagNameFromCache(spaceName, tagId)
                print('tagName: ', tagName)
                tagItem = self.metaClient.getTagItemFromCache(spaceName, tagName)
                print('tagItem: ', tagItem)
                schemaVersion = tagItem.version
                print('schemaVersion: ', schemaVersion)
                rowReaders[tagId] = RowReader(schema, schemaVersion)
                rows[tagName] = [] ###
                tagIdNameMap[tagId] = tagName
        else:
            print('scanVertexResponse.vertex_schema is None')

        if scanVertexResponse.vertex_data is not None:
            print('start to process vertex_data')
            for scanTag in scanVertexResponse.vertex_data:
                tagId = scanTag.tagId
                if tagId not in rowReaders.keys():
                    continue

                rowReader = rowReaders[tagId]
                defaultProperties = rowReader.vertexKey(scanTag.vertexId, scanTag.tagId)
                properties = rowReader.decodeValue(scanTag.value)
                tagName = tagIdNameMap[tagId]
                rows[tagName].append(Row(defaultProperties, properties))
        else:
            print('scanVertexResponse.vertex_data is None')

        return Result(rows)
