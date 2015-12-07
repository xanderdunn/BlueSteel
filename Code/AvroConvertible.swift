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
        return .AvroStringValue(self)
    }
}

extension Bool: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return .AvroBooleanValue(self)
    }
}

extension Int: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return .AvroLongValue(Int64(self))
    }
}

extension Int32: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return .AvroIntValue(self)
    }
}

extension Int64: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return .AvroLongValue(self)
    }
}

extension Float: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return .AvroFloatValue(self)
    }
}

extension Double: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return .AvroDoubleValue(self)
    }
}
