//
//  Boxes.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

public final class Box<T> {
    private let _value : () -> T

    public init(_ value : T) {
        self._value = { value }
    }

    public var value: T {
        return _value()
    }

    public func map<U>(f: T -> U) -> Box<U> {
        return Box<U>(f(value))
    }
}
