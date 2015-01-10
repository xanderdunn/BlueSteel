//
//  AvroValueEncoding.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

/// Avro value binary encoding.
public extension AvroValue {

    /**
    AvroValue binary encoding.

    :param schemaData Avro JSON schema as NSData.

    :returns Avro binary encoding as byte array. Nil if encoding fails.
    */
    public func encode(schemaData: NSData) -> [Byte]? {
        let schema = Schema(schemaData)
        return self.encode(schema)
    }

    /**
    AvroValue binary encoding.

    :param jsonSchema Avro JSON schema string.

    :returns Avro binary encoding as byte array. Nil if encoding fails.
    */
    public func encode(jsonSchema: String) -> [Byte]? {
        let schema = Schema(jsonSchema)
        return self.encode(schema)
    }

    /**
    AvroValue binary encoding.

    :param schema Avro schema object.

    :returns Avro binary encoding as byte array. Nil if encoding fails.
    */
    public func encode(schema: Schema) -> [Byte]? {
        let encoder = AvroEncoder()
        return self.encode(encoder, schema: schema)
    }

    /**
    AvroValue binary encoding.

    :param encoder Avro Encoder.
    :param schema Avro schema object.

    :returns Avro binary encoding as byte array. Nil if encoding fails.
    */
    public func encode(encoder: AvroEncoder, schema: Schema) -> [Byte]? {
        switch schema {
            case .AvroNullSchema :
                encoder.emitNull()

            case .AvroBooleanSchema :
                switch self {
                case .AvroBooleanValue(let value) :
                    encoder.emitBool(value)
                default :
                    return nil
                }

            case .AvroIntSchema :
                switch self {
                case .AvroIntValue(let value) :
                    encoder.emitInt32(value)
                default :
                    return nil
                }

            case .AvroLongSchema :
                switch self {
                case .AvroIntValue(let value) :
                    encoder.emitInt64(Int64(value))
                case .AvroLongValue(let value) :
                    encoder.emitInt64(value)
                default :
                    return nil
                }

            case .AvroFloatSchema :
                switch self {
                case .AvroIntValue(let value) :
                    encoder.emitFloat(Float(value))
                case .AvroLongValue(let value) :
                    encoder.emitFloat(Float(value))
                case .AvroFloatValue(let value) :
                    encoder.emitFloat(value)
                default :
                    return nil
                }

            case .AvroDoubleSchema :
                switch self {
                case .AvroIntValue(let value) :
                    encoder.emitDouble(Double(value))
                case .AvroLongValue(let value) :
                    encoder.emitDouble(Double(value))
                case .AvroFloatValue(let value) :
                    encoder.emitDouble(Double(value))
                case .AvroDoubleValue(let value) :
                    encoder.emitDouble(value)
                default :
                    return nil
                }

            case .AvroStringSchema, .AvroBytesSchema :
                switch self {
                case .AvroStringValue(let value) :
                    encoder.emitString(value)
                case .AvroBytesValue(let value) :
                    encoder.emitBytes(value)
                default :
                    return nil
                }

        case .AvroArraySchema(let box) :
            switch self {
            case .AvroArrayValue(let values) :
                if values.count != 0 {
                    encoder.emitInt64(Int64(values.count))
                    for value in values {
                        if value.encode(encoder, schema: box.value) == nil {
                            return nil
                        }
                    }
                }
                encoder.emitInt64(0)
            default :
                return nil
            }

        case .AvroMapSchema(let box) :
            switch self {
            case .AvroMapValue(let pairs) :
                if pairs.count != 0 {
                    encoder.emitInt64(Int64(pairs.count))
                    for key in pairs.keys {
                        encoder.emitString(key)
                        if let value = pairs[key] {
                            if value.encode(encoder, schema: box.value) == nil {
                                return nil
                            }
                        } else {
                            return nil
                        }
                    }
                }
                encoder.emitInt64(0)
            default :
                return nil
            }

        case .AvroRecordSchema(let name, let fieldSchemas) :
            switch self {
            case .AvroRecordValue(let pairs) :
                for fSchema in fieldSchemas {
                    switch fSchema {
                    case .AvroFieldSchema(let key, let box) :
                        if let value = pairs[key] {
                            if value.encode(encoder, schema: box.value) == nil {
                                return nil
                            }
                        } else {
                            // Since we don't support schema defaults, fail encoding when values are missing for schema keys.
                            return nil
                        }
                    default :
                        return nil
                    }
                }
            default :
                return nil
            }

        case .AvroEnumSchema(let name, let enumSchemas) :
            switch self {
            //TODO: Make sure enum matches schema
            case .AvroEnumValue(let index, _) :
                encoder.emitInt32(Int32(index))
            default :
                return nil
            }

        case .AvroUnionSchema(let uSchemas) :
            switch self {
            case .AvroUnionValue(let index, let box) :
                encoder.emitInt64(Int64(index))
                if index < uSchemas.count {
                    if box.value.encode(encoder, schema: uSchemas[index]) == nil {
                        return nil
                    }
                } else {
                    return nil
                }
            default :
                return nil
            }


        case .AvroFixedSchema(_, let size) :
            switch self {
            case .AvroFixedValue(let fixedBytes) :
                if fixedBytes.count == size {
                    encoder.emitFixed(fixedBytes)
                } else {
                    return nil
                }
            default :
                return nil
            }

        default :
            return nil
        }
        return encoder.bytes
    }
}
