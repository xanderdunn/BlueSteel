//
//  AvroConvertible.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

public protocol AvroValueConvertible {
    func toAvro() -> AvroValue
}

extension String: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return .avroStringValue(self)
    }
}

extension Bool: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return .avroBooleanValue(self)
    }
}

extension Int: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return .avroLongValue(Int64(self))
    }
}

extension Int32: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return .avroIntValue(self)
    }
}

extension Int64: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return .avroLongValue(self)
    }
}

extension Float: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return .avroFloatValue(self)
    }
}

extension Double: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return .avroDoubleValue(self)
    }
}
