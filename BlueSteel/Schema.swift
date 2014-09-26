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
    case PrimitiveSchema(AvroType)
    case ArraySchema(Box<Schema>)
    case MapSchema(Box<Schema>)
    case UnionSchema(Array<Schema>)

    // Named Types
    case FixedSchema(String, Int)
    case EnumSchema(String, Array<String>)
    case RecordSchema(String, Array<Schema>)
    case FieldSchema(String, Box<Schema>)

    // TODO: Report errors for invalid schemas.
    case InvalidSchema

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
        case .PrimitiveSchema(let aType) :
            switch aType {
            case .ANull :
                return "\"null\""
            case .ABoolean :
                return "\"boolean\""
            case .AInt :
                return "\"int\""
            case .ALong :
                return "\"long\""
            case .AFloat :
                return "\"float\""
            case .ADouble :
                return "\"double\""
            case .AString :
                return "\"string\""
            case .ABytes :
                return "\"bytes\""
            default :
                return nil
            }

        case .ArraySchema(let boxed) :
            return "{\"type\":\"array\",\"items\":\(boxed.value.parsingCanonicalForm(&existingTypes)))}"

        case .MapSchema(let boxed) :
            return "{\"type\":\"array\",\"values\":\(boxed.value.parsingCanonicalForm(&existingTypes))}"

        case .EnumSchema(let name, let enumValues) :
            if contains(existingTypes, name) {
                return name
            } else {
                existingTypes.append(name)
                var str = "{\"name\":\"\(name)\",\"type\":\"enum\",\"symbols\":["
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

        case .RecordSchema(let name, let fields) :
            if contains(existingTypes, name) {
                return name
            } else {
                existingTypes.append(name)
                var str = "{\"name\":\"\(name)\",\"type\":\"record\",\"fields\":["
                var first = true
                for field in fields {
                    if !first {
                        str += ","
                    } else {
                        first = false
                    }

                    switch field {
                    case .FieldSchema(let fieldName, let fieldType) :
                        str += "{\"name\":\"\(fieldName)\",\"type\":\(fieldType.value.parsingCanonicalForm(&existingTypes))}"
                    default :
                        return nil
                    }
                }
                str += "]}"
                return str
            }

        case .FixedSchema(let name, let size) :
            if contains(existingTypes, name) {
                return name
            } else {
                existingTypes.append(name)
                return "{\"name\":\"\(name)\",\"type\":\"fixed\",\"size\":[\(size)]}"
            }

        default :
            return nil
        }
    }

    init(_ json: Dictionary<String, JSONValue>) {
        // Stub
        self = .InvalidSchema
    }

    public init(_ json: NSData) {
        self = Schema(JSONValue(json), typeKey:"type")
    }

    public init(_ json: String) {
        self = Schema(JSONValue(json.dataUsingEncoding(NSUTF8StringEncoding, allowLossyConversion: false)), typeKey:"type")
    }

    init(_ json: JSONValue, typeKey key: String) {
        switch json[key] {
        case .JString(let typeString) :
            let avroType = AvroType(typeString)

            switch avroType {
            case .ABoolean, .AInt, .ALong, .AFloat, .ADouble, .AString, .ANull, .ABytes :
                self = .PrimitiveSchema(AvroType(typeString))

            case .AMap :
                let schema = Schema(json, typeKey: "values")

                switch schema {
                case .InvalidSchema :
                    self = .InvalidSchema
                default :
                    self = .MapSchema(Box(schema))
                }

            case .AArray :
                let schema = Schema(json, typeKey: "items")

                switch schema {
                case .InvalidSchema :
                    self = .InvalidSchema
                default :
                    self = .ArraySchema(Box(schema))
                }

            case .ARecord :
                // Records must be named
                if let recordName = Schema.assembleFullName(json["namespace"].string , name: json["name"].string) {
                    switch json["fields"] {
                    case .JArray(let fields) :
                        var recordFields: [Schema] = []

                        for field in fields {
                            // Fields must be named
                            if let fieldName = field["name"].string {
                                let schema = Schema(field, typeKey: "type")

                                switch schema {
                                case .InvalidSchema :
                                    self = .InvalidSchema
                                    return

                                default :
                                    recordFields.append(.FieldSchema(fieldName, Box(schema)))
                                }
                            } else {
                                self = .InvalidSchema
                                return
                            }
                        }
                        self = .RecordSchema(recordName, recordFields)
                    default :
                        self = .InvalidSchema
                    }
                } else {
                    self = .InvalidSchema
                }

            case .AEnum :
                if let enumName = Schema.assembleFullName(json["namespace"].string, name: json["name"].string) {
                    println(enumName)
                    switch json["symbols"] {
                    case .JArray(let symbols) :
                        var symbolStrings: [String] = []
                        for sym in symbols {
                            if let symbol = sym.string {
                                symbolStrings.append(symbol)
                            } else {
                                self = .InvalidSchema
                                return
                            }
                        }

                        self = .EnumSchema(enumName, symbolStrings)
                    default :
                        self = .InvalidSchema
                    }
                } else {
                    self = .InvalidSchema
                }

            case .AFixed :
                if let fixedName = Schema.assembleFullName(json["namespace"].string, name: json["name"].string) {
                    if let size = json["size"].integer {
                        self = .FixedSchema(fixedName, size)
                        return
                    }
                }
                self = .InvalidSchema

            default:
                // Schema type is invalid
                self = .InvalidSchema
            }

        case .JObject(_):
            self = Schema(json[key], typeKey: "type")

        // Union
        case .JArray(let unionSchema):
            var schemas: [Schema] = []
            for def in unionSchema {
                var schema: Schema
                switch def {
                case .JString(let typeString) :
                    let avroType = AvroType(typeString)
                    if  avroType != .AInvalidType {
                        schema = .PrimitiveSchema(avroType)
                    } else {
                        schema = .InvalidSchema
                    }
                case .JArray(_) :
                    // Nested unions not permitted
                    schema = .InvalidSchema
                default :
                    schema = Schema(def, typeKey: "type")
                }

                switch schema {
                case .InvalidSchema:
                    self = .InvalidSchema
                default:
                    schemas.append(schema)
                }
            }
            self = .UnionSchema(schemas)

        default:
            self = .InvalidSchema
        }
    }

}
