from enum import Enum

class PropertyDef:
    PropertyType = Enum('PropertyType', ('UNKNOWN', 'BOOL', 'INT', 'VID', 'FLOAT', 'DOUBLE', \
        'STRING', 'VERTEX_ID', 'TAG_ID', 'SRC_ID', 'EDGE_TYPE', 'EDGE_RANK', 'DST_ID')) # 类静态变量

    def __init__(self, propertyType, name):
        self.propertyType = propertyType
        self.name = name


class Property:
    def __init__(self, propertyType, name, value):
        self.propertyDef = PropertyDef(propertyType, name)
        self.value = value
    
    def getPropertyType(self):
        return self.propertyDef.type
    
    def getName(self):
        return self.propertyDef.name
    
    def getValue(self):
        return self.valiue 

class Row:
    def __init__(self, defaultProperties, properties):
        self.defaultProperties = defaultProperties
        self.properties = properties


class RowReader:
    def __init__(self, schema, schemaVersion=0):
        self.schemaVersion = schemaVersion
        self.defs = []
        self.propertyNameIndex = {}
        self.propertyTypes = []

        idx = 0
        for columnDef in schema.columns:
            propertyType = PropertyDef.PropertyType(columnDef.type.type+1) # ColumnDef is in common/ttypes.py
            print('propertyType: ', propertyType)
            columnName = columnDef.name
            self.defs.append((columnName, propertyType))
            self.propertyNameIndex[columnName] = idx
            idx = idx + 1

    def decodeValue(self, value, schemaVersion=None):
            if schemaVersion is None:
                schemaVersion = self.schemaVersion
            properties = []
            for i in range(len(self.defs)):
                field = self.defs[i][0]
                propertyType = self.defs[i][1]
                data = value #decodeResult[i] #######????????
                if propertyType == PropertyDef.PropertyType.BOOL:
                    properties.append(self.getBoolProperty(field, data))
                elif propertyType == PropertyDef.PropertyType.INT or propertyType == PropertyDef.PropertyType.VID:
                    properties.append(self.getIntProperty(field, data))
                elif propertyType == PropertyDef.PropertyType.FLOAT:
                    properties.append(self.getFloatProperty(field, data))
                elif propertyType == PropertyDef.PropertyType.DOUBLE:
                    properties.append(self.getDoubleProperty(field, data))
                elif propertyType == PropertyDef.PropertyType.STRING:
                    properties.append(self.getStringProperty(field, data))
                else:
                    # exception: "Invalid type in schema: type"
                    raise Exception('Invalid propertyType in schema: ', propertyType)

            return properties

    def edgeKey(self, srcId, edgeType, dstId):
        properties = []
        properties.append(Property(PropertyDef.PropertyType.SRC_ID, "_srcId", srcId))
        properties.append(Property(PropertyDef.PropertyType.EDGE_TYPE, "_edgeType", edgeType))
        properties.append(Property(PropertyDef.PropertyType.DST_ID, "_dstId", dstId))
        return properties

    def decodeEdgeKey(self, key):  # key是byte[]
        properties = []
        buffer = wrap(key)
        
        properties.append(Property(PropertyDef.PropertyType.SRC_ID, "_srcId", buffer.getLong()))
        properties.append(Property(PropertyDef.PropertyType.EDGE_TYPE, "_edgeType", buffer.getInt()))
        properties.append(Property(PropertyDef.PropertyType.EDGE_RANK, "_rank", buffer.getLong()))
        properties.append(Property(PropertyDef.PropertyType.DST_ID, "_dstId", buffer.getLong()))

        return properties

    def getProperty(self, row, name):
        if name not in propertyNameIndex.keys():
            return None
        return row.properties[propertyNameIndex[name]]

    def getPropertyByIndex(self, row, index):
        if index < 0 or index >= len(row.getProperties()):
            return None
        return row.properties[index]

    def getBoolProperty(self, name, data):
        value = data[0] != 0x00 
        return Property(PropertyDef.PropertyType.BOOL, name, value)

    def getIntProperty(self, name, data):
        return Property(PropertyDef.PropertyType.INT, name, data)  #### 字节流解析出data

    def getFloatProperty(self, name, data):
        return Property(PropertyDef.PropertyType.FLOAT, name, data)

    def getDoubleProperty(self, name, data):
        return Property(PropertyDef.PropertyType.DOUBLE, name, data)

    def getStringProperty(self, name, data):
        return Property(PropertyDef.PropertyType.STRING, name, data)


class Result:
    def __init__(self, rows):
        self.rows = rows
        self.size = 0
        for edgeName in rows.keys():
            self.size += len(rows[edgeName])
