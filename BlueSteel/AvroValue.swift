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
    case AvroUnionValue(Int, Box<AvroValue>)
    case AvroRecordValue(Dictionary<String, AvroValue>)
    case AvroEnumValue(Int, String)
    case AvroFixedValue([Byte])

    case AvroInvalidValue

    public var boolean: Bool? {
        switch self {
        case .AvroBooleanValue(let value) :
            return value
        case .AvroUnionValue(_, let box) :
            return box.value.boolean
        default :
            return nil
        }
    }

    public var string: String? {
        switch self {
        case .AvroStringValue(let value) :
            return value
        case .AvroUnionValue(_, let box) :
            return box.value.string
        default :
            return nil
        }
    }

    public var integer: Int32? {
        switch self {
        case .AvroIntValue(let value) :
            return value
        case .AvroUnionValue(_, let box) :
            return box.value.integer
        default :
            return nil
        }
    }

    public var long: Int64? {
        switch self {
        case .AvroLongValue(let value) :
            return value
        case .AvroUnionValue(_, let box) :
            return box.value.long
        default :
            return nil
        }
    }

    public var float: Float? {
        switch self {
        case .AvroFloatValue(let value) :
            return value
        case .AvroUnionValue(_, let box) :
            return box.value.float
        default :
            return nil
        }
    }

    public var double: Double? {
        switch self {
        case .AvroDoubleValue(let value) :
            return value
        case .AvroUnionValue(_, let box) :
            return box.value.double
        default :
            return nil
        }
    }

    public var bytes: [Byte]? {
        switch self {
        case .AvroBytesValue(let value) :
            return value
        case .AvroUnionValue(_, let box) :
            return box.value.bytes
        default :
            return nil
        }
    }

    public var array: Array<AvroValue>? {
        switch self {
        case .AvroArrayValue(let values) :
            return values
        case .AvroUnionValue(_, let box) :
            return box.value.array
        default :
            return nil
        }
    }

    public var map: Dictionary<String, AvroValue>? {
        switch self {
        case .AvroMapValue(let pairs) :
            return pairs
        case .AvroUnionValue(_, let box) :
            return box.value.map
        default :
            return nil
        }
    }

    public var record: Dictionary<String, AvroValue>? {
        switch self {
        case .AvroRecordValue(let fields) :
            return fields
        case .AvroUnionValue(_, let box) :
            return box.value.record
        default :
            return nil
        }
    }

    public var enumeration: String? {
        switch self {
        case .AvroEnumValue(_, let value) :
            return value
        case .AvroUnionValue(_, let box) :
            return box.value.enumeration
        default :
            return nil
        }
    }

    public var fixed: [Byte]? {
        switch self {
        case .AvroFixedValue(let bytes) :
            return bytes
        case .AvroUnionValue(_, let box) :
            return box.value.fixed
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
            if let key = AvroValue(.AvroStringSchema, withDecoder: decoder).string {
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

    public init(jsonSchema: String, withBytes bytes: [Byte]) {
        let avroData = NSData(bytes: UnsafePointer<Void>(bytes), length: bytes.count)

        self.init(jsonSchema: jsonSchema, withData: avroData)
    }

    public init(jsonSchema: String, withData data: NSData) {
        let schema = Schema(jsonSchema)

        self.init(schema: schema, withData: data)
    }

    public init(schema: Schema, withBytes bytes: [Byte]) {
        let avroData = NSData(bytes: UnsafePointer<Void>(bytes), length: bytes.count)

        self.init(schema: schema, withData: avroData)
    }

    public init(schema: Schema, withData data: NSData) {
        let decoder = AvroDecoder(data)

        self.init(schema, withDecoder: decoder)
    }

    init(_ schema: Schema, withDecoder decoder: AvroDecoder) {

        switch schema {
        case .AvroNullSchema :
            self = .AvroNullValue

        case .AvroBooleanSchema :
            if let decoded = decoder.decodeBoolean() {
                self = .AvroBooleanValue(decoded)
            } else {
                self = .AvroInvalidValue
            }

        case .AvroIntSchema :
            if let decoded = decoder.decodeInt32() {
                self = .AvroIntValue(decoded)
            } else {
                self = .AvroInvalidValue
            }

        case .AvroLongSchema :
            if let decoded = decoder.decodeInt64() {
                self = .AvroLongValue(decoded)
            } else {
                self = .AvroInvalidValue
            }

        case .AvroFloatSchema :
            if let decoded = decoder.decodeFloat() {
                self = .AvroFloatValue(decoded)
            } else {
                self = .AvroInvalidValue
            }

        case .AvroDoubleSchema :
            if let decoded = decoder.decodeDouble() {
                self = .AvroDoubleValue(decoded)
            } else {
                self = .AvroInvalidValue
            }

        case .AvroStringSchema :
            if let decoded = decoder.decodeString() {
                self = .AvroStringValue(decoded)
            } else {
                self = .AvroInvalidValue
            }

        case .AvroBytesSchema :
            if let decoded = decoder.decodeBytes() {
                self = .AvroBytesValue(decoded)
            } else {
                self = .AvroInvalidValue
            }

        // TODO: Collections negative count support.
        case .AvroArraySchema(let boxedSchema) :
            var values: [AvroValue] = []
            while let count = decoder.decodeInt64() {
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


        case .AvroMapSchema(let boxedSchema) :
            var pairs: Dictionary<String, AvroValue> = [:]
            while let count = decoder.decodeInt64() {
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


        case .AvroEnumSchema(_, let enumValues) :
            if let index = decoder.decodeInt32() {
                if Int(index) > enumValues.count - 1 {
                    self = .AvroEnumValue(Int(index), enumValues[Int(index)])
                    return
                }
            }
            self = .AvroInvalidValue

        case .AvroRecordSchema(_, let fields) :
            var pairs: Dictionary<String, AvroValue> = [:]
            for field in fields {
                switch field {
                case .AvroFieldSchema(let key, let box) :
                    pairs[key] = AvroValue(box.value, withDecoder: decoder)
                default :
                    self = .AvroInvalidValue
                    return
                }
            }
            self = .AvroRecordValue(pairs)

        case .AvroFixedSchema(_, let size) :
            if let bytes = decoder.decodeFixed(size) {
                self = .AvroFixedValue(bytes)
            } else {
                self = .AvroInvalidValue
            }

        case .AvroUnionSchema(let schemas) :
            if let index = decoder.decodeInt64() {
                if Int(index) < schemas.count {
                    self = .AvroUnionValue(Int(index), Box(AvroValue(schemas[Int(index)], withDecoder: decoder)))
                    return
                }
            }
            self = .AvroInvalidValue

        default :
            self = .AvroInvalidValue
        }
    }
}

extension AvroValue: NilLiteralConvertible {
    public init(nilLiteral: ()) {
        self = AvroNullValue
    }
}

extension AvroValue: BooleanLiteralConvertible {
    public init(booleanLiteral value: Bool) {
        self = .AvroBooleanValue(value)
    }
}

extension AvroValue: IntegerLiteralConvertible {
    public init(integerLiteral value: IntegerLiteralType) {
        self = .AvroLongValue(Int64(value))
    }
}

extension AvroValue: FloatLiteralConvertible {
    public init(floatLiteral value: FloatLiteralType) {
        self = .AvroDoubleValue(value)
    }
}

extension AvroValue: StringLiteralConvertible {

    public init(unicodeScalarLiteral value: String) {
        self = AvroInvalidValue
    }

    public init(extendedGraphemeClusterLiteral value: String) {
        self = .AvroStringValue(value)
    }

    public init(stringLiteral value: String) {
        self = .AvroStringValue(value)
    }
}

extension AvroValue: ArrayLiteralConvertible {
    public init(arrayLiteral elements: AvroValue...) {
        self = .AvroArrayValue(elements)
    }
}

extension AvroValue: DictionaryLiteralConvertible {

    public init(dictionaryLiteral elements:(String, AvroValue)...) {
        var tmp = [String:AvroValue](minimumCapacity: elements.count)
        for kv in elements {
            tmp[kv.0] = kv.1
        }
        self = .AvroMapValue(tmp)
    }
}