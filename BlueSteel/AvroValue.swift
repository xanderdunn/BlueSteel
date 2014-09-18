//
//  AvroValue.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation


extension Dictionary {

}

func +=<K, V> (inout left: Dictionary<K, V>, right: Dictionary<K, V>) -> Dictionary<K, V> {
    for (k, v) in right {
        left.updateValue(v, forKey: k)
    }
    return left
}

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
    case AvroEnumValue(Int32, String)
    case AvroFixedValue([Byte])

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
        case .AvroMapValue(let pairs) :
            return pairs
        default :
            return nil
        }
    }

    public var record: Dictionary<String, AvroValue>? {
        switch self {
        case .AvroRecordValue(let fields) :
            return fields
        default :
            return nil
        }
    }

    public var enumeration: String? {
        switch self {
        case .AvroEnumValue(_, let value) :
            return value
        default :
            return nil
        }
    }

    public var fixed: [Byte]? {
        switch self {
        case .AvroFixedValue(let bytes) :
            return bytes
        default :
            return nil
        }
    }

    static func decodeArrayBlock(schema: Schema, count:Int64, decoder: AvroDecoder) -> [AvroValue]? {
        var values: [AvroValue] = []
        for idx in 0...count - 1 {
            let value = AvroValue(schema, withDecoder: decoder)
            switch value {
            case .AvroInvalidValue :
                return nil
            default :
                values.append(value)
            }
        }
        return values
    }

    static func decodeMapBlock(schema: Schema, count:Int64, decoder: AvroDecoder) -> Dictionary<String, AvroValue>? {
        var pairs: Dictionary<String, AvroValue> = Dictionary()
        for idx in 0...count - 1 {
            if let key = AvroValue(.PrimitiveSchema(.AString), withDecoder: decoder).string {
                let value = AvroValue(schema, withDecoder: decoder)
                switch value {
                case .AvroInvalidValue :
                    return nil
                default :
                    pairs[key] = value
                }
            } else {
                return nil
            }
        }
        return pairs
    }

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

        // TODO: Collections negative count support.
        case .ArraySchema(let boxedSchema) :
            var values: [AvroValue] = []
            while let count = decoder.decodeLong() {
                if count == 0 {
                    self = .AvroArrayValue(values)
                    return
                }

                if let block = AvroValue.decodeArrayBlock(boxedSchema.value, count: count, decoder: decoder) {
                    values += block
                } else {
                    self = .AvroInvalidValue
                    return
                }
            }
            self = .AvroInvalidValue


        case .MapSchema(let boxedSchema) :
            var pairs: Dictionary<String, AvroValue> = [:]
            while let count = decoder.decodeLong() {
                if count == 0 {
                    self = .AvroMapValue(pairs)
                    return
                }
                if let block = AvroValue.decodeMapBlock(boxedSchema.value, count: count, decoder: decoder) {
                    pairs += block
                } else {
                    self = .AvroInvalidValue
                    return
                }
            }
            self = .AvroInvalidValue


        case .EnumSchema(_, let enumValues) :
            if let index = decoder.decodeInt() {
                if Int(index) > enumValues.count - 1 {
                    self = .AvroEnumValue(index, enumValues[Int(index)])
                    return
                }
            }
            self = .AvroInvalidValue

        case .RecordSchema(_, let fields) :
            var pairs: Dictionary<String, AvroValue> = [:]
            for field in fields {
                switch field {
                case .FieldSchema(let key, let box) :
                    pairs[key] = AvroValue(box.value, withDecoder: decoder)
                default :
                    self = .AvroInvalidValue
                    return
                }
            }
            self = .AvroRecordValue(pairs)

        case .FixedSchema(_, let size) :
            if let bytes = decoder.decodeFixed(size) {
                self = .AvroFixedValue(bytes)
            } else {
                self = .AvroInvalidValue
            }

        case .UnionSchema(let schemas) :
            if let index = decoder.decodeLong() {
                if Int(index) < schemas.count {
                    self = AvroValue(schemas[Int(index)], withDecoder: decoder)
                    return
                }
            }
            self = .AvroInvalidValue

        default :
            self = .AvroInvalidValue
        }
    }
}