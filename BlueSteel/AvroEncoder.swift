//
//  AvroEncoder.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

public class AvroEncoder {

    var bytes: [UInt8] = []

    func encodeNull() {
        return
    }

    func encodeBoolean(value: Bool) {
        if value {
            bytes.append(UInt8(0x1))
        } else {
            bytes.append(UInt8(0x0))
        }
        return
    }

    func encodeInt(value: Int32) {
        let encoded = Varint(fromValue: Int64(value).encodeZigZag())
        bytes += encoded.backing
        return
    }

    func encodeLong(value: Int64) {
        let encoded = Varint(fromValue: value.encodeZigZag())
        bytes += encoded.backing
        return
    }
    
    func encodeFloat(value: Float) {
        let bits: UInt32 = unsafeBitCast(value, UInt32.self)

        let encodedFloat = [UInt8(0xff & bits),
            UInt8(0xff & (bits >> 8)),
            UInt8(0xff & (bits >> 16)),
            UInt8(0xff & (bits >> 24))]

        bytes += encodedFloat
        return
    }
    
    func encodeDouble(value: Double) {
        let bits: UInt64 = unsafeBitCast(value, UInt64.self)

        let encodedDouble = [UInt8(0xff & bits),
            UInt8(0xff & (bits >> 8)),
            UInt8(0xff & (bits >> 16)),
            UInt8(0xff & (bits >> 24)),
            UInt8(0xff & (bits >> 32)),
            UInt8(0xff & (bits >> 40)),
            UInt8(0xff & (bits >> 48)),
            UInt8(0xff & (bits >> 56))]
        bytes += encodedDouble
        return
    }

    func encodeString(value: String) {
        var cstr = value.cStringUsingEncoding(NSUTF8StringEncoding)!
        let bufferptr = UnsafeBufferPointer<UInt8>(start: UnsafePointer<UInt8>(cstr), count: cstr.count - 1)

        let stringBytes = [UInt8](bufferptr)
        encodeBytes(stringBytes)
        return
    }

    func encodeBytes(value: [UInt8]) {
        encodeLong(Int64(value.count))
        bytes += value
        return
    }

    func encodeFixed(value: [UInt8]) {
        bytes += value
        return
    }

    public init() {
        bytes = []
    }
}
