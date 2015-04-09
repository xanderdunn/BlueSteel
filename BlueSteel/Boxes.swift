//
//  Boxes.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

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

class BoxedArray<T>: MutableCollectionType, Sliceable {
    typealias Element = T
    typealias SubSlice = ArraySlice<T>

    var boxed: Array<T>

    var startIndex: Int {
        return boxed.startIndex
    }

    var endIndex: Int {
        return boxed.endIndex
    }

    init(_ array:Array<T>) {
        boxed = array
    }

    func generate() -> IndexingGenerator<[T]> {
        return boxed.generate()
    }

    subscript (index: Int) -> T {
        get {
            return boxed[index]
            }

            set {
                boxed[index] = newValue
            }
    }

    subscript (subRange: Range<Int>) -> ArraySlice<T> {
        get {
            return boxed[subRange]
        }
    }
}
