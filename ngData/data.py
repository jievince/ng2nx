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
        return propertyDef.type
    
    def getName(self):
        return propertyDef.name
    
    def getValue(self):
        propertyType = propertyDef.type
        if propertyType == BOOL: # python 类型转换
            return bool(value)
        elif propertyType == INT or propertyType == VID or propertyType == VERTEX_ID \
                or propertyType == SRC_ID or propertyType == DST_ID or propertyType == EDGE_RANK:
            return long(value)
        elif propertyType == TAG_ID or propertyType == EDGE_TYPE:
            return int(value)
        elif propertyType == FLOAT:
            return float(value)
        elif propertyType == DOUBLE:
            return double(value)
        elif propertyType == STRING:
            return string(value)
        else:
            return None
        

class Row:
    def __init__(self, defaultProperties, properties):
        self.defaultproperties = defaultProperties
        self.properties = properties


class RowReader:
    def __init__(self, schema, schemaVersion=0):
        self.schemaVersion = schemaVersion
        self.defs = []
        self.propertyNameIndex = {}
        self.propertyTypes = []

        idx = 0
        for columnDef in schema.columns:
            propertyType = PropertyType.getEnum(columnDef.type.type) # ColumnDef is in common/ttypes.py
            columnName = columnDef.name
            if propertyType == BOOL:
                defs.append((name, BOOL)) # 需要convert to class???
            elif propertyType == INT or columnName == VID:
                defs.append((name, LONG))
            elif propertyType == FLOAT:
                defs.append((name, FLOAT))
            elif propertyType == DOUBLE:
                defs.append((name, DOUBLE))
            elif propertyType == STRING:
                defs.append((name, STRING))
            else:
                # exception: "Invalid type in schema: type"
                pass

            propertyTypes.append(propertyType)
            propertyNameIndex[columnName] = idx
            idx = idx + 1

    def docodeValue(self, value, schemaVersion):
            properties = [Property() for _ in range(len(defs))] #
            for i in len(defs):
                field = defs[i].getField()
                propertyType = propertyTypes[i]
                data = decodeResult[i]
                if propertyTypes[i] == BOOL:
                    properties[i] = getBoolProperty(field, data)
                elif propertyTypes[i] == INT or propertyTypes[i] == VID:
                    properties[i] = getIntProperty(field, data)
                elif propertyTypes[i] == FLOAT:
                    properties[i] = getFloatProperty(field, data)
                elif propertyTypes[i] == DOUBLE:
                    properties[i] = getDoubleProperty(field, data)
                elif propertyTypes[i] == STRING:
                    properties[i] = getStringProperty(field, data)
                else:
                    # exception: "Invalid type in schema: type"
                    pass

            return properties

    def edgeKey(self, srcId, edgeType, dstId):
        properties = []
        properties.append(Property(PropertyType.SRC_ID, "_srcId", srcId))
        properties.append(Property(PropertyType.EDGE_TYPE, "_edgeType", edgeType))
        properties.append(Property(PropertyType.DST_ID, "_dstId", dstId))
        return properties

    def decodeEdgeKey(self, key):  # key是byte[]
        properties = []
        buffer = wrap(key)
        
        properties.append(Property(PropertyType.SRC_ID, "_srcId", buffer.getLong()))
        properties.append(Property(PropertyType.EDGE_TYPE, "_edgeType", buffer.getInt()))
        properties.append(Property(PropertyType.EDGE_RANK, "_rank", buffer.getLong()))
        properties.append(Property(PropertyType.DST_ID, "_dstId", buffer.getLong()))

        return properties

    def getProperty(self, row, name):
        if name not in propertyNameIndex.keys():
            return None
        return row.getProperties()[propertyNameIndex[name]]

    def getPropertyByIndex(self, row, index):
        if index < 0 or index >= len(row.getProperties()):
            return None
        return row.getProperties()[index]

    def getBoolProperty(self, name, data):
        value = data[0] != 0x00 
        return Property(PropertyType.BOOL, name, value)

    def getIntProperty(self, name, data):
        return Property(PropertyType.INT, name, buffer.getLong())

    def getFloatProperty(self, name, data):
        return Property(PropertyType.FLOAT, name, buffer.getFloat())

    def getDoubleProperty(self, name, data):
        return Property(PropertyType.DOUBLE, name, buffer.getDouble())

    def getStringProperty(self, name, data):
        return Property(PropertyType.STRING, name, String(buffer.array()))


class Result:
    def __init__(self, rows):
        self.rows = rows
        self.size = 0
        for edgeName in rows.keys():
            self.size += len(rows[edgeName])
