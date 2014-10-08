//
//  AvroValueEncoding.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

public extension AvroValue {

    public func encode(encoder: AvroEncoder, schema: Schema) -> [Byte]? {
        switch schema {
        case .PrimitiveSchema(let aType) :
            switch aType {
            case .ANull :
                encoder.encodeNull()

            case .ABoolean :
                switch self {
                case .AvroBooleanValue(let value) :
                    encoder.encodeBoolean(value)
                default :
                    return nil
                }

            case .AInt :
                switch self {
                case .AvroIntValue(let value) :
                    encoder.encodeInt(value)
                default :
                    return nil
                }

            case .ALong :
                switch self {
                case .AvroIntValue(let value) :
                    encoder.encodeLong(Int64(value))
                case .AvroLongValue(let value) :
                    encoder.encodeLong(value)
                default :
                    return nil
                }

            case .AFloat :
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

            case .ADouble :
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

            case .AString, .ABytes :
                switch self {
                case .AvroStringValue(let value) :
                    encoder.encodeString(value)
                case .AvroBytesValue(let value) :
                    encoder.encodeBytes(value)
                default :
                    return nil
                }
            default :
                return nil
            }

        case .ArraySchema(let box) :
            switch self {
            case .AvroArrayValue(let values) :
                encoder.encodeLong(Int64(values.count))
                for value in values {
                    if value.encode(encoder, schema: box.value) == nil {
                        return nil
                    }
                }
                encoder.encodeLong(0)
            default :
                return nil
            }

        case .MapSchema(let box) :
            switch self {
            case .AvroMapValue(let pairs) :
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
            default :
                return nil
            }

        case .RecordSchema(let name, let fieldSchemas) :
            switch self {
            case .AvroRecordValue(let pairs) :
                for fSchema in fieldSchemas {
                    switch fSchema {
                    case .FieldSchema(let key, let box) :
                        if let value = pairs[key] {
                            encoder.encodeString(key)
                            if value.encode(encoder, schema: box.value) == nil {
                                return nil
                            }
                        }
                    default :
                        return nil
                    }
                }
            default :
                return nil
            }

        case .EnumSchema(let name, let enumSchemas) :
            switch self {
            //TODO: Make sure enum matches schema
            case .AvroEnumValue(let index, _) :
                encoder.encodeInt(index)
            default :
                return nil
            }

        case .UnionSchema(let uSchemas) :
            switch self {
            case .AvroUnionValue(let index, let box) :
                encoder.encodeLong(index)
                if box.value.encode(encoder, schema: schema) == nil {
                    return nil
                }
            default :
                return nil
            }

        // Dont forget fixed schema
        default :
            return nil
        }
        return encoder.bytes
    }
}
