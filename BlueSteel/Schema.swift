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
    case FixedSchema(String)
    case EnumSchema(String)
    case RecordSchema(String, Array<Schema>)
    case FieldSchema(String, Box<Schema>)

    // TODO: Report errors for invalid schemas.
    case InvalidSchema


    init(_ json: Dictionary<String, JSONValue>) {
        self = .InvalidSchema
    }

    public init(_ json:String) {
        self = Schema(JSONValue(json.dataUsingEncoding(NSUTF8StringEncoding, allowLossyConversion: false)))
    }

    public init(_ json: JSONValue) {
        var schemaType = json["type"]
        var schemaName = json["name"]
        println(schemaType.description)
        switch json["type"] {

        case .JString(let typeString) :
            let avroType = AvroType(typeString)

            switch avroType {
            case .ABoolean, .AInt, .ALong, .AFloat, .ADouble, .AString, .ANull, .ABytes :
                self = .PrimitiveSchema(AvroType(typeString))

            case .AMap :
                switch json["values"] {
                case .JString(let valueType) :
                    if AvroType(valueType) != .AInvalidType {
                        self = .MapSchema(Box(.PrimitiveSchema(AvroType(valueType))))
                    } else {
                        self = .InvalidSchema
                    }

                case .JObject(let subSchema) :
                    self = .MapSchema(Box(Schema(json["values"])))

                case .JArray(let unionSchema) :
                    // TODO: Map of union.
                    self = .InvalidSchema

                default:
                    self = .InvalidSchema
                }

            case .AArray :
                switch json["items"] {
                case .JString(let valueType) :
                    if AvroType(valueType) != .AInvalidType {
                        self = .ArraySchema(Box(.PrimitiveSchema(AvroType(valueType))))
                    } else {
                        self = .InvalidSchema
                    }

                case .JObject(let subSchema) :
                    self = .ArraySchema(Box(Schema(json["items"])))

                case .JArray(let unionSchema) :
                    self = .InvalidSchema

                default:
                    self = .InvalidSchema
                }

            case .ARecord :
                // Record stub
                self = .InvalidSchema

            case .AEnum :
                // Enum stub
                self = .InvalidSchema

            default:
                // Schema type is invalid
                self = .InvalidSchema
            }

        case .JObject(let subSchema):
            self = Schema(schemaType)

        // Union
        case .JArray(let unionSchema):
            self = .UnionSchema([.InvalidSchema])
            var schemas: [Schema] = []
            for def in unionSchema {
                let schema = Schema(def)
                switch schema {
                    case .InvalidSchema:
                        self = .InvalidSchema
                        return
                    default:
                        schemas.append(schema)
                }
            }
            self = .UnionSchema(schemas)

        default:
            self = .InvalidSchema

            return
        }
    }

}
