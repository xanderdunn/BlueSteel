//
//  AvroEncoder.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

public class AvroEncoder {

    var bytes: [Byte] = []

    func encodeNull() {
        return
    }

    func encodeBoolean(value: Bool) {
        if value {
            bytes.append(Byte(0x1))
        } else {
            bytes.append(Byte(0x0))
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

        let encodedFloat = [Byte(0xff & (bits >> 24)),
        Byte(0xff & (bits >> 16)),
        Byte(0xff & (bits >> 8)),
        Byte(0xff & bits)]

        bytes += encodedFloat
        return
    }
    
    func encodeDouble(value: Double) {
        let bits: UInt64 = unsafeBitCast(value, UInt64.self)

        let encodedDouble = [Byte(0xff & (bits >> 56)),
            Byte(0xff & (bits >> 48)),
            Byte(0xff & (bits >> 40)),
            Byte(0xff & (bits >> 32)),
            Byte(0xff & (bits >> 24)),
            Byte(0xff & (bits >> 16)),
            Byte(0xff & (bits >> 8)),
            Byte(0xff & bits)]
        bytes += encodedDouble
        return
    }

    func encodeString(value: String) {
        var cstr = value.cStringUsingEncoding(NSUTF8StringEncoding)!
        let bufferptr = UnsafeBufferPointer<Byte>(start: UnsafePointer<Byte>(cstr), count: cstr.count - 1)

        let stringBytes = [Byte](bufferptr)
        encodeBytes(stringBytes)
        return
    }

    func encodeBytes(value: [Byte]) {
        encodeLong(Int64(value.count))
        bytes += value
        return
    }

    func encodeFixed(value: [Byte]) {
        bytes += value
        return
    }

    public init() {
        bytes = []
    }
}
