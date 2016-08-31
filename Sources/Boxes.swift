//
//  Boxes.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

public final class Box<T> {
  fileprivate let _value : () -> T

  public init(_ value : T) {
    self._value = { value }
  }

  public var value: T {
    return _value()
  }

  public func map<U>(_ f: (T) -> U) -> Box<U> {
    return Box<U>(f(value))
  }
}

class BoxedArray<T>: MutableCollection, Collection {
    typealias Index = Int
    typealias Element = T

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

    func makeIterator() -> IndexingIterator<[T]> {
        return boxed.makeIterator()
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

        set {
            boxed[subRange] = newValue
        }
    }




//    associatedtype Index : Comparable
//
//
//    /// Accesses the element at the specified position.
//    ///
//    /// For example, you can replace an element of an array by using its
//    /// subscript.
//    ///
//    ///     var streets = ["Adams", "Bryant", "Channing", "Douglas", "Evarts"]
//    ///     streets[1] = "Butler"
//    ///     print(streets[1])
//    ///     // Prints "Butler"
//    ///
//    /// You can subscript a collection with any valid index other than the
//    /// collection's end index. The end index refers to the position one
//    /// past the last element of a collection, so it doesn't correspond with an
//    /// element.
//    ///
//    /// - Parameter position: The position of the element to access. `position`
//    ///   must be a valid index of the collection that is not equal to the
//    ///   `endIndex` property.
//    public subscript(position: Self.Index) -> Self._Element { get set }
//
//    /// A collection that represents a contiguous subrange of the collection's
//    /// elements.
//    associatedtype SubSequence
//
//    /// Accesses a contiguous subrange of the collection's elements.
//    ///
//    /// The accessed slice uses the same indices for the same elements as the
//    /// original collection. Always use the slice's `startIndex` property
//    /// instead of assuming that its indices start at a particular value.
//    ///
//    /// This example demonstrates getting a slice of an array of strings, finding
//    /// the index of one of the strings in the slice, and then using that index
//    /// in the original array.
//    ///
//    ///     let streets = ["Adams", "Bryant", "Channing", "Douglas", "Evarts"]
//    ///     let streetsSlice = streets[2 ..< streets.endIndex]
//    ///     print(streetsSlice)
//    ///     // Prints "["Channing", "Douglas", "Evarts"]"
//    ///
//    ///     let index = streetsSlice.index(of: "Evarts")    // 4
//    ///     streets[index!] = "Eustace"
//    ///     print(streets[index!])
//    ///     // Prints "Eustace"
//    ///
//    /// - Parameter bounds: A range of the collection's indices. The bounds of
//    ///   the range must be valid indices of the collection.
//    public subscript(bounds: Range<Self.Index>) -> Self.SubSequence { get set }
//
    /// Returns the position immediately after the given index.
    ///
    /// - Parameter i: A valid index of the collection. `i` must be less than
    ///   `endIndex`.
    /// - Returns: The index value immediately after `i`.
    public func index(after i: Index)
        -> Index
    {
        return i + 1
    }

    /// Replaces the given index with its successor.
    ///
    /// - Parameter i: A valid index of the collection. `i` must be less than
    ///   `endIndex`.
    public func formIndex(after i: inout Index)
    {
        i = index(after: i)
    }
}
