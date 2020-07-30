from enum import Enum
import struct

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
        self.offset = 0

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
            self.offset = 0
            header = value[0]
            self.offset += 1
            for i in range(len(self.defs)):
                field = self.defs[i][0]
                propertyType = self.defs[i][1]
                #data = value #decodeResult[i] #######????????
                if propertyType == PropertyDef.PropertyType.BOOL:
                    print('$$$$$$$$$$$$$$decodeValue: BOOL', ' offset: ', self.offset)
                    properties.append(self.getBoolProperty(field, value))
                elif propertyType == PropertyDef.PropertyType.INT or propertyType == PropertyDef.PropertyType.VID:
                    print('$$$$$$$$$$$$$decodeValue: INT or VID', ' offset: ', self.offset)
                    properties.append(self.getIntProperty(field, value))
                elif propertyType == PropertyDef.PropertyType.FLOAT:
                    print('$$$$$$$$$$$decodeValue: FLOAT', 'offset: ', self.offset)
                    properties.append(self.getFloatProperty(field, value))
                elif propertyType == PropertyDef.PropertyType.DOUBLE:
                    print('$$$$$$$$$$decodeValue: DOUBLE', 'offset: ', self.offset)
                    properties.append(self.getDoubleProperty(field, value))
                elif propertyType == PropertyDef.PropertyType.STRING:
                    print('$$$$$$$$$$decodeValue: STRING', 'offset: ', self.offset)
                    properties.append(self.getStringProperty(field, value))
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

    def vertexKey(self, vertexId, tagId):
        properties = []
        properties.append(Property(PropertyDef.PropertyType.VERTEX_ID, "_vertexId", vertexId))
        properties.append(Property(PropertyDef.PropertyType.TAG_ID, "_tagId", tagId))
        return properties

    def getProperty(self, row, name):
        if name not in propertyNameIndex.keys():
            return None
        return row.properties[propertyNameIndex[name]]

    def getPropertyByIndex(self, row, index):
        if index < 0 or index >= len(row.getProperties()):
            return None
        return row.properties[index]

    def getBoolProperty(self, name, value):
        val = value[self.offset] != 0x00
        self.offset += 1
        return Property(PropertyDef.PropertyType.BOOL, name, val)

    def getIntProperty(self, name, value):
        val = self.readCompressedInt(value)
        print('parsing int: ', val)
        return Property(PropertyDef.PropertyType.INT, name, val)  #### 字节流解析出data

    def getFloatProperty(self, name, value):
        val = 0.0
        self.offset += 4
        return Property(PropertyDef.PropertyType.FLOAT, name, val)

    def getDoubleProperty(self, name, value):
        val = struct.unpack_from('<d', value, self.offset)[0]
        self.offset += 8
        return Property(PropertyDef.PropertyType.DOUBLE, name, val)

    def getStringProperty(self, name, value):
        strLen = self.readCompressedInt(value)
        val = str(value[self.offset:self.offset+strLen], 'utf-8')
        self.offset += strLen
        return Property(PropertyDef.PropertyType.STRING, name, val)
    
    def readCompressedInt(self, value):
        shift = 0
        val = 0
        curOff = self.offset
        byteV = 0
        while curOff < len(value):
            byteV = struct.unpack_from('b', value, curOff)[0]
            print('curByte: ', value[curOff], 'byteV: ', byteV)
            if byteV >= 0:
                break
            val |= (byteV & 0x7f) << shift
            curOff += 1
            shift += 7
        if curOff == len(value):
            return None
        val |= byteV << shift
        curOff += 1
        print('readCompressedInt: ', value[self.offset:curOff], 'val is: ', val)
        self.offset = curOff
        return val


class Result:
    def __init__(self, rows):
        self.rows = rows
        self.size = 0
        for entry in rows:
            self.size += len(rows[entry])
