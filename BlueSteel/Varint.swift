//
//  Varint.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

// MARK: Varint

public struct Varint {
    let backing: [Byte] = []

    public var count: Int {
        return backing.count
    }

    public var description: String {
        get {
            return backing.description
        }
    }

    // This initializer should never be used directly.
    // VarintFromBytes should be used instead.

    init(fromBytes bytes: [Byte]) {
        backing = bytes
    }

    public init(fromValue value:UInt) {
        var tmp = value
        var idx: UInt = 0
        while tmp > 0 {
            backing.append(UInt8(truncate: tmp))
            if (idx > 0) {
                backing[idx - 1] |= 0x80
            }

            // Next index
            idx++
            tmp >>= 7
        }
    }

    public func toInt() -> Int {
        return Int(bitPattern: self.toUInt())
    }

    public func toUInt() -> UInt {
        var result: UInt = 0

        for var idx:Int = 0; idx < backing.count; idx++ {
            let tmp:UInt8 = backing[idx]

            result |= UInt(tmp & 0x7F) << UInt(7 * idx)
        }
        return result
    }

    public static func VarintFromBytes(bytes: [Byte]) -> Varint? {
        var buf = [Byte]()

        for x in bytes {
            buf.append(x)

            if ((x & 0x80) == 0) {
                break
            }
        }
        if (buf.count > 0) {
            return Varint(fromBytes: buf)
        }
        return nil
    }
}

extension Varint {

    public init(fromValue value:Int) {
        self = Varint(fromValue: UInt(bitPattern: value))
    }

}

// MARK: - Integer extensions

extension UInt8 {

    init(truncate val:UInt) {
        self.init(val & 0xFF)
    }

    init(truncate val:UInt64) {
        self.init(val & 0xFF)
    }

    init(truncate val:UInt32) {
        self.init(val & 0xFF)
    }

    init(truncate val:UInt16) {
        self.init(val & 0xFF)
    }

}

extension UInt16 {

    init(truncate val:UInt) {
        self.init(val & 0xFFFF)
    }

    init(truncate val:UInt64) {
        self.init(val & 0xFFFF)
    }

    init(truncate val:UInt32) {
        self.init(val & 0xFFFF)
    }

    init(truncate val:UInt16) {
        self.init(val & 0xFFFF)
    }
}

// MARK: Zig Zag encoding

extension Int {
    public func encodeZigZag() -> UInt {
        let encoded = ((self << 1) ^ (self >> (sizeof(Int) * 8 - 1)))
        return UInt(bitPattern:  encoded)
    }
}

extension UInt {
    public func decodeZigZag() -> Int {
        let decoded = ((self & 0x1) * UInt.max) ^ (self >> 1)
        return Int(bitPattern: decoded)
    }
}
