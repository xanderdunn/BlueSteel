//
//  Schema.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

public enum AvroType {
    // Primitives
    case ANull
    case ABoolean
    case AInt
    case ALong
    case AFloat
    case ADouble
    case AString
    case ABytes

    // Complex
    case AEnum
    case AFixed
    case ARecord
    case AArray
    case AMap

    // Invalid
    case AInvalidType

    init(_ typeString: String) {

        if typeString == "boolean" {
            self = .ABoolean
        } else if typeString == "int" {
            self = .AInt
        } else if typeString == "long" {
            self = .ALong
        } else if typeString == "float"  {
            self = .AFloat
        } else if typeString == "double" {
            self = .ADouble
        } else if typeString == "string" {
            self = .AString
        } else if typeString == "bytes" {
            self = .ABytes
        } else if typeString == "enum" {
            self = .AEnum
        } else if typeString == "fixed" {
            self = .AFixed
        } else if typeString == "record" {
            self = .ARecord
        } else if typeString == "array" {
            self = .AArray
        } else if typeString == "map" {
            self = .AMap
        } else if typeString == "null" {
            self = .ANull
        } else {
            self = .AInvalidType
        }
        return
    }
}

public enum Schema {
    case AvroNullSchema
    case AvroBooleanSchema
    case AvroIntSchema
    case AvroLongSchema
    case AvroFloatSchema
    case AvroDoubleSchema
    case AvroStringSchema
    case AvroBytesSchema

    case AvroArraySchema(Box<Schema>)
    case AvroMapSchema(Box<Schema>)
    case AvroUnionSchema(Array<Schema>)

    // Named Types
    case AvroFixedSchema(String, Int)
    case AvroEnumSchema(String, Array<String>)
    case AvroRecordSchema(String, Array<Schema>)
    case AvroFieldSchema(String, Box<Schema>)

    // TODO: Report errors for invalid schemas.
    case AvroInvalidSchema

    static func assembleFullName(namespace:String?, name: String?) -> String? {
        if let shortName = name {
            if !contains(shortName, ".") {
                if let space = namespace {
                    return space + "." + shortName
                }
            }
            return shortName
        } else {
            return nil
        }
    }

    public func parsingCanonicalForm(inout existingTypes: [String]) -> String? {
        switch self {
        case .AvroNullSchema :
            return "\"null\""
        case .AvroBooleanSchema :
            return "\"boolean\""
        case .AvroIntSchema :
            return "\"int\""
        case .AvroLongSchema :
            return "\"long\""
        case .AvroFloatSchema :
            return "\"float\""
        case .AvroDoubleSchema :
            return "\"double\""
        case .AvroStringSchema :
            return "\"string\""
        case .AvroBytesSchema :
            return "\"bytes\""


        case .AvroArraySchema(let boxed) :
            if let arrayPCF = boxed.value.parsingCanonicalForm(&existingTypes) {
                return "{\"type\":\"array\",\"items\":" + arrayPCF + "}"
            } else {
                return nil
            }

        case .AvroMapSchema(let boxed) :
            if let mapPCF = boxed.value.parsingCanonicalForm(&existingTypes) {
                return "{\"type\":\"map\",\"values\":" + mapPCF + "}"
            } else {
                return nil
            }

        case .AvroEnumSchema(let name, let enumValues) :
            if contains(existingTypes, name) {
                return "\"" + name + "\""
            } else {
                existingTypes.append(name)
                var str = "{\"name\":\"" + name + "\",\"type\":\"enum\",\"symbols\":["
                var first = true
                for val in enumValues {
                    if first {
                        str += "\"\(val)\""
                        first = false
                    } else {
                        str += ",\"\(val)\""
                    }
                }
                str += "]}"
                return str
            }

        case .AvroRecordSchema(let name, let fields) :
            if contains(existingTypes, name) {
                return "\"" + name + "\""
            } else {
                existingTypes.append(name)
                var str = "{\"name\":\"" + name + "\",\"type\":\"record\",\"fields\":["
                var first = true
                for field in fields {
                    if !first {
                        str += ","
                    } else {
                        first = false
                    }

                    switch field {
                    case .AvroFieldSchema(let fieldName, let fieldType) :
                        if let fieldPCF = fieldType.value.parsingCanonicalForm(&existingTypes) {
                            str += "{\"name\":\"" + fieldName + "\",\"type\":" + fieldPCF + "}"
                        } else {
                            println(fieldName)
                            return nil
                        }
                    default :
                        return nil
                    }
                }
                str += "]}"
                return str
            }

        case .AvroFixedSchema(let name, let size) :
            if contains(existingTypes, name) {
                return "\"" + name + "\""
            } else {
                existingTypes.append(name)
                return "{\"name\":\"" + name + "\",\"type\":\"fixed\",\"size\":\(size)}"
            }

        case .AvroUnionSchema(let unionSchemas) :
            var str = "["
            var first = true
            for uschema in unionSchemas {
                if !first {
                    str += ","
                } else {
                    first = false
                }

                if let unionPCF = uschema.parsingCanonicalForm(&existingTypes) {
                    str += unionPCF
                } else {
                    return nil
                }
            }
            str += "]"
            return str
        default :
            return nil
        }
    }

    public func fingerprint() -> String? {
        var etypes: [String] = []
        if let pcf = self.parsingCanonicalForm(&etypes) {
            var hash = [Byte](count: 32, repeatedValue: 0)
            if let cString = pcf.cStringUsingEncoding(NSUTF8StringEncoding) {
                // Compute hash of PCF string without the NULL terminator.

                BlueSteel_SHA256(cString, UInt32(cString.count - 1), &hash)
                var hexBits = "" as String
                for value in hash {
                    hexBits += NSString(format:"%02X", value) as String
                }
                return hexBits
            }
        }
        return nil
    }

    init(_ json: Dictionary<String, JSONValue>) {
        // Stub
        self = .AvroInvalidSchema
    }

    public init(_ json: NSData) {
        var cached: [String:Schema] = [:]
        self = Schema(JSONValue(json), typeKey:"type", namespace: nil, cachedSchemas: &cached)
    }

    public init(_ json: String) {
        var cached: [String:Schema] = [:]
        self = Schema(JSONValue(json.dataUsingEncoding(NSUTF8StringEncoding, allowLossyConversion: false)), typeKey:"type", namespace: nil, cachedSchemas: &cached)
    }

    init(_ json: JSONValue, typeKey key: String, namespace ns: String?, inout cachedSchemas cache: [String:Schema]) {
        var schemaNamespace: String?
        if let jsonNamespace = json["namespace"].string {
            schemaNamespace = jsonNamespace
        } else {
            schemaNamespace = ns
        }

        switch json[key] {
        case .JString(let typeString) :
            let avroType = AvroType(typeString)

            switch avroType {
            case .ABoolean :
                self = .AvroBooleanSchema
            case .AInt :
                self = .AvroIntSchema
            case .ALong :
                self = .AvroLongSchema
            case .AFloat :
                self = .AvroFloatSchema
            case .ADouble :
                self = .AvroDoubleSchema
            case .AString :
                self = .AvroStringSchema
            case .ANull :
                self = .AvroNullSchema
            case .ABytes :
                self = .AvroBytesSchema

            case .AMap :
                let schema = Schema(json, typeKey: "values", namespace: schemaNamespace, cachedSchemas: &cache)

                switch schema {
                case .AvroInvalidSchema :
                    self = .AvroInvalidSchema
                default :
                    self = .AvroMapSchema(Box(schema))
                }

            case .AArray :
                let schema = Schema(json, typeKey: "items", namespace: schemaNamespace, cachedSchemas: &cache)

                switch schema {
                case .AvroInvalidSchema :
                    self = .AvroInvalidSchema
                default :
                    self = .AvroArraySchema(Box(schema))
                }

            case .ARecord :
                // Records must be named
                if let recordName = Schema.assembleFullName(schemaNamespace , name: json["name"].string) {
                    switch json["fields"] {
                    case .JArray(let fields) :
                        var recordFields: [Schema] = []

                        for field in fields {
                            // Fields must be named
                            if let fieldName = field["name"].string {
                                let schema = Schema(field, typeKey: "type", namespace: schemaNamespace, cachedSchemas: &cache)

                                switch schema {
                                case .AvroInvalidSchema :
                                    self = .AvroInvalidSchema
                                    return

                                default :
                                    recordFields.append(.AvroFieldSchema(fieldName, Box(schema)))
                                }
                            } else {
                                self = .AvroInvalidSchema
                                return
                            }
                        }
                        self = .AvroRecordSchema(recordName, recordFields)
                        cache[recordName] = self
                    default :
                        self = .AvroInvalidSchema
                    }
                } else {
                    self = .AvroInvalidSchema
                }

            case .AEnum :
                if let enumName = Schema.assembleFullName(schemaNamespace, name: json["name"].string) {
                    switch json["symbols"] {
                    case .JArray(let symbols) :
                        var symbolStrings: [String] = []
                        for sym in symbols {
                            if let symbol = sym.string {
                                symbolStrings.append(symbol)
                            } else {
                                self = .AvroInvalidSchema
                                return
                            }
                        }

                        self = .AvroEnumSchema(enumName, symbolStrings)
                        cache[enumName] = self
                    default :
                        self = .AvroInvalidSchema
                    }
                } else {
                    self = .AvroInvalidSchema
                }

            case .AFixed :
                if let fixedName = Schema.assembleFullName(schemaNamespace, name: json["name"].string) {
                    if let size = json["size"].integer {
                        self = .AvroFixedSchema(fixedName, size)
                        cache[fixedName] = self
                        return
                    }
                }
                self = .AvroInvalidSchema

            default:
                // Schema type is invalid
                if let cachedSchema = cache[typeString] {
                    self = cachedSchema
                } else {
                    self = .AvroInvalidSchema
                }
            }

        case .JObject(_):
            self = Schema(json[key], typeKey: "type", namespace: schemaNamespace, cachedSchemas: &cache)

        // Union
        case .JArray(let unionSchema):
            var schemas: [Schema] = []
            for def in unionSchema {
                var schema: Schema = .AvroInvalidSchema
                switch def {
                case .JString(let typeString) :
                    let avroType = AvroType(typeString)
                    if  avroType != .AInvalidType {
                        //schema = .PrimitiveSchema(avroType)
                        switch avroType {
                        case .ABoolean :
                            schema = .AvroBooleanSchema
                        case .AInt :
                            schema = .AvroIntSchema
                        case .ALong :
                            schema = .AvroLongSchema
                        case .AFloat :
                            schema = .AvroFloatSchema
                        case .ADouble :
                            schema = .AvroDoubleSchema
                        case .AString :
                            schema = .AvroStringSchema
                        case .ANull :
                            schema = .AvroNullSchema
                        case .ABytes :
                            schema = .AvroBytesSchema
                        default :
                            schema = .AvroInvalidSchema
                        }
                    } else if let cachedSchema = cache[typeString] {
                        schema = cachedSchema
                    } else {
                        schema = .AvroInvalidSchema
                    }


                case .JArray(_) :
                    // Nested unions not permitted
                    schema = .AvroInvalidSchema
                default :
                    schema = Schema(def, typeKey: "type", namespace: schemaNamespace, cachedSchemas: &cache)
                }

                switch schema {
                case .AvroInvalidSchema:
                    self = .AvroInvalidSchema
                    return
                default:
                    schemas.append(schema)
                }
            }
            self = .AvroUnionSchema(schemas)

        default:
            self = .AvroInvalidSchema
        }
    }

}

extension Schema : Equatable {

}

public func ==(lhs: Schema, rhs: Schema) -> Bool {
    switch (lhs) {
    case .AvroBooleanSchema :
        switch rhs {
        case .AvroBooleanSchema :
            return true
        default :
            return false
        }

    case .AvroIntSchema :
        switch rhs {
        case .AvroIntSchema :
            return true
        default :
            return false
        }
    case .AvroLongSchema :
        switch rhs {
        case .AvroLongSchema :
            return true
        default :
            return false
        }

    case .AvroFloatSchema :
        switch rhs {
        case .AvroFloatSchema :
            return true
        default :
            return false
        }

    case .AvroDoubleSchema :
        switch rhs {
        case .AvroDoubleSchema :
            return true
        default :
            return false
        }

    case .AvroBytesSchema :
        switch rhs {
        case .AvroBytesSchema :
            return true
        default :
            return false
        }

    case .AvroStringSchema :
        switch rhs {
        case .AvroStringSchema :
            return true
        default :
            return false
        }

    case .AvroNullSchema :
        switch rhs {
        case .AvroNullSchema :
            return true
        default :
            return false
        }

    case .AvroArraySchema(let lbox) :
        switch rhs {
        case .AvroArraySchema(let rbox) :
            if lbox.value == rbox.value {
                return true
            }
            return false
        default :
            return false
        }

    case .AvroMapSchema(let lbox) :
        switch rhs {
        case .AvroMapSchema(let rbox) :
            if lbox.value == rbox.value {
                return true
            }
            return false
        default :
            return false
        }

    case .AvroRecordSchema(let lRecordName, let lRecordSchemas) :
        switch rhs {
        case .AvroRecordSchema(let rRecordName, let rRecordSchemas) :
            if (lRecordName == rRecordName) && (lRecordSchemas.count == rRecordSchemas.count) {
                for idx in 0..<lRecordSchemas.count {
                    if lRecordSchemas[idx] != rRecordSchemas[idx] {
                        return false
                    }
                }
                return true
            }
            return false
        default :
            return false
        }

    case .AvroFieldSchema(let lFieldName, let lFieldBox) :
        switch rhs {
        case .AvroFieldSchema(let rFieldName, let rFieldBox) :
            if (lFieldName == rFieldName) && (lFieldBox.value == rFieldBox.value) {
                return true
            }
            return false
        default :
            return false
        }

    case .AvroUnionSchema(let lUnionSchemas) :
        switch rhs {
        case .AvroUnionSchema(let rUnionSchemas) :
            if lUnionSchemas.count == rUnionSchemas.count {
                for idx in 0..<lUnionSchemas.count {
                    if lUnionSchemas[idx] != rUnionSchemas[idx] {
                        return false
                    }
                }
                return true
            }
            return false
        default :
            return false
        }

    case .AvroEnumSchema(let lEnumName, let lSymbols) :
        switch rhs {
        case .AvroEnumSchema(let rEnumName, let rSymbols) :
            if (lEnumName == rEnumName) && (lSymbols.count == rSymbols.count) {
                for idx in 0..<lSymbols.count {
                    if lSymbols[idx] != rSymbols[idx] {
                        return false
                    }
                }
                return true
            }
            return false
        default :
            return false
        }

    case .AvroFixedSchema(let lFixedName, let lSize) :
        switch rhs {
        case .AvroFixedSchema(let rFixedName, let rSize) :
            if (lFixedName == rFixedName) && (lSize == rSize) {
                return true
            }
            return false
        default :
            return false
        }

    default :
        return false
    }
}
