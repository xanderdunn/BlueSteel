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
