//
//  AvroValue.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation


public enum AvroValue {
    // Primitives
    case AvroNullValue
    case AvroBooleanValue(Bool)
    case AvroIntValue(Int32)
    case AvroLongValue(Int64)
    case AvroFloatValue(Float)
    case AvroDoubleValue(Double)
    case AvroBytesValue([Byte])
    case AvroStringValue(String)

    // Complex Types
    case AvroArrayValue([AvroValue])
    case AvroMapValue(Dictionary<String, AvroValue>)
    case AvroRecordValue(Dictionary<String, AvroValue>)

    case AvroInvalidValue

    public var boolean: Bool? {
        switch self {
        case .AvroBooleanValue(let value) :
            return value
        default :
            return nil
        }
    }

    public var string: String? {
        switch self {
        case .AvroStringValue(let value) :
            return value
        default :
            return nil
        }
    }

    public var integer: Int32? {
        switch self {
        case .AvroIntValue(let value) :
            return value
        default :
            return nil
        }
    }

    public var long: Int64? {
        switch self {
        case .AvroLongValue(let value) :
            return value
        default :
            return nil
        }
    }

    public var float: Float? {
        switch self {
        case .AvroFloatValue(let value) :
            return value
        default :
            return nil
        }
    }

    public var double: Double? {
        switch self {
        case .AvroDoubleValue(let value) :
            return value
        default :
            return nil
        }
    }

    public var bytes: [Byte]? {
        switch self {
        case .AvroBytesValue(let value) :
            return value
        default :
            return nil
        }
    }

    public var array: Array<AvroValue>? {
        switch self {
        case .AvroArrayValue(let values) :
            return values
        default :
            return nil
        }
    }

    public var map: Dictionary<String, AvroValue>? {
        switch self {
        case .AvroMapValue(let values) :
            return values
        default :
            return nil
        }
    }

    public var record: Dictionary<String, AvroValue>? {
        return nil
    }

    public var enumeration: String? {
        return nil
    }

    // TODO: Deal with fixed.

    public init(jsonSchema: String, withBytes bytes:[Byte]) {
        let schema = Schema(jsonSchema)
        let avroData = NSData(bytes: UnsafePointer<Void>(bytes), length: bytes.count)
        let decoder = AvroDecoder(avroData)

        self = AvroValue(schema, withDecoder: decoder)
    }

    init(_ schema: Schema, withDecoder decoder: AvroDecoder) {

        switch schema {
        case .PrimitiveSchema(.ANull) :
            self = .AvroNullValue

        case .PrimitiveSchema(.ABoolean) :
            if let decoded = decoder.decodeBoolean() {
                self = .AvroBooleanValue(decoded)
            } else {
                self = .AvroInvalidValue
            }

        case .PrimitiveSchema(.AInt) :
            if let decoded = decoder.decodeInt() {
                self = .AvroIntValue(decoded)
            } else {
                self = .AvroInvalidValue
            }

        case .PrimitiveSchema(.ALong) :
            if let decoded = decoder.decodeLong() {
                self = .AvroLongValue(decoded)
            } else {
                self = .AvroInvalidValue
            }

        case .PrimitiveSchema(.AFloat) :
            if let decoded = decoder.decodeFloat() {
                self = .AvroFloatValue(decoded)
            } else {
                self = .AvroInvalidValue
            }

        case .PrimitiveSchema(.ADouble) :
            if let decoded = decoder.decodeDouble() {
                self = .AvroDoubleValue(decoded)
            } else {
                self = .AvroInvalidValue
            }

        case .PrimitiveSchema(.AString) :
            if let decoded = decoder.decodeString() {
                self = .AvroStringValue(decoded)
            } else {
                self = .AvroInvalidValue
            }

        case .PrimitiveSchema(.ABytes) :
            if let decoded = decoder.decodeBytes() {
                self = .AvroBytesValue(decoded)
            } else {
                self = .AvroInvalidValue
            }

        case .ArraySchema(let boxedSchema) :
            if let count = decoder.decodeLong() {
                var values: [AvroValue] = []
                for idx in 0...count - 1 {
                    let value = AvroValue(boxedSchema.value, withDecoder: decoder)
                    switch value {
                    default :
                        values.append(value)
                    }
                }
                if let terminator = decoder.decodeLong() {
                    if terminator == 0 {
                        self = .AvroArrayValue(values)
                        return
                    }
                }
            }
            self = .AvroInvalidValue


        case .MapSchema(let boxedSchema) :
            self = .AvroInvalidValue

        case .EnumSchema(let boxedSchema) :
            self = .AvroInvalidValue

        case .RecordSchema(let boxedSchema) :
            self = .AvroInvalidValue

        case .FixedSchema(let name, let size) :
            self = .AvroInvalidValue

        default :
            self = .AvroInvalidValue
        }
    }
}