//
//  BoxedArray.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

class BoxedArray<T>: MutableCollectionType, Sliceable {
    typealias Element = T
    typealias SubSlice = Slice<T>

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

    subscript (subRange: Range<Int>) -> Slice<T> {
        get {
            return boxed[subRange]
        }
    }
}
