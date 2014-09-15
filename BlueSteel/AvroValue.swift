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
    case array([AvroValue])
    case map(Dictionary<String, AvroValue>)
    case record(Dictionary<String, AvroValue>)

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

    public init(jsonSchema: String, withBytes bytes:[Byte]) {
        let schema = Schema(jsonSchema)
        let avroData = NSData(bytes: UnsafePointer<Void>(bytes), length: bytes.count)
        self = AvroValue(schema, withData: avroData)
    }

    init(_ schema: Schema, withData data: NSData) {
        let decoder = AvroDecoder(data)

        switch schema {
        case .PrimitiveSchema(.ANull) :
            self = .AvroNullValue

        case .PrimitiveSchema(.ABoolean) :
            if let decoded = decoder.decodeBoolean() {
                self = .AvroBooleanValue(decoded)
            } else {
                self = .AvroInvalidValue
            }

        case .PrimitiveSchema(.AInt):
            if let decoded = decoder.decodeInt() {
                self = .AvroIntValue(decoded)
            } else {
                self = .AvroInvalidValue
            }

        case .PrimitiveSchema(.ALong):
            if let decoded = decoder.decodeLong() {
                self = .AvroLongValue(decoded)
            } else {
                self = .AvroInvalidValue
            }

        case .PrimitiveSchema(.AFloat):
            if let decoded = decoder.decodeFloat() {
                self = .AvroFloatValue(decoded)
            } else {
                self = .AvroInvalidValue
            }

        case .PrimitiveSchema(.ADouble):
            if let decoded = decoder.decodeDouble() {
                self = .AvroDoubleValue(decoded)
            } else {
                self = .AvroInvalidValue
            }

        case .PrimitiveSchema(.AString):
            if let decoded = decoder.decodeString() {
                self = .AvroStringValue(decoded)
            } else {
                self = .AvroInvalidValue
            }

        case .PrimitiveSchema(.ABytes):
            if let decoded = decoder.decodeBytes() {
                self = .AvroBytesValue(decoded)
            } else {
                self = .AvroInvalidValue
            }

        default :
            self = .AvroInvalidValue
        }
    }
}