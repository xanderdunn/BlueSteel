//
//  AvroValueEncoding.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

public extension AvroValue {

    public func encode(schemaData: NSData) -> [Byte]? {
        let schema = Schema(schemaData)
        return self.encode(schema)
    }

    public func encode(jsonSchema: String) -> [Byte]? {
        let schema = Schema(jsonSchema)
        return self.encode(schema)
    }

    public func encode(schema: Schema) -> [Byte]? {
        let encoder = AvroEncoder()
        return self.encode(encoder, schema: schema)
    }

    public func encode(encoder: AvroEncoder, schema: Schema) -> [Byte]? {
        switch schema {

            case .AvroNullSchema :
                encoder.encodeNull()

            case .AvroBooleanSchema :
                switch self {
                case .AvroBooleanValue(let value) :
                    encoder.encodeBoolean(value)
                default :
                    return nil
                }

            case .AvroIntSchema :
                switch self {
                case .AvroIntValue(let value) :
                    encoder.encodeInt(value)
                default :
                    return nil
                }

            case .AvroLongSchema :
                switch self {
                case .AvroIntValue(let value) :
                    encoder.encodeLong(Int64(value))
                case .AvroLongValue(let value) :
                    encoder.encodeLong(value)
                default :
                    return nil
                }

            case .AvroFloatSchema :
                switch self {
                case .AvroIntValue(let value) :
                    encoder.encodeFloat(Float(value))
                case .AvroLongValue(let value) :
                    encoder.encodeFloat(Float(value))
                case .AvroFloatValue(let value) :
                    encoder.encodeFloat(value)
                default :
                    return nil
                }

            case .AvroDoubleSchema :
                switch self {
                case .AvroIntValue(let value) :
                    encoder.encodeDouble(Double(value))
                case .AvroLongValue(let value) :
                    encoder.encodeDouble(Double(value))
                case .AvroFloatValue(let value) :
                    encoder.encodeDouble(Double(value))
                case .AvroDoubleValue(let value) :
                    encoder.encodeDouble(value)
                default :
                    return nil
                }

            case .AvroStringSchema, .AvroBytesSchema :
                switch self {
                case .AvroStringValue(let value) :
                    encoder.encodeString(value)
                case .AvroBytesValue(let value) :
                    encoder.encodeBytes(value)
                default :
                    return nil
                }

        case .AvroArraySchema(let box) :
            switch self {
            case .AvroArrayValue(let values) :
                if values.count != 0 {
                    encoder.encodeLong(Int64(values.count))
                    for value in values {
                        if value.encode(encoder, schema: box.value) == nil {
                            return nil
                        }
                    }
                }
                encoder.encodeLong(0)
            default :
                return nil
            }

        case .AvroMapSchema(let box) :
            switch self {
            case .AvroMapValue(let pairs) :
                if pairs.count != 0 {
                    encoder.encodeLong(Int64(pairs.count))
                    for key in pairs.keys {
                        encoder.encodeString(key)
                        if let value = pairs[key] {
                            if value.encode(encoder, schema: box.value) == nil {
                                return nil
                            }
                        } else {
                            return nil
                        }
                    }
                }
                encoder.encodeLong(0)
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
                encoder.encodeInt(Int32(index))
            default :
                return nil
            }

        case .AvroUnionSchema(let uSchemas) :
            switch self {
            case .AvroUnionValue(let index, let box) :
                encoder.encodeLong(Int64(index))
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
                    encoder.encodeFixed(fixedBytes)
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
