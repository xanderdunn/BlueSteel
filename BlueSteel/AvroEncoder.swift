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

    func encodeBoolean(value: Boolean) {
        bytes.append(Byte(value))
        return
    }

    func encodeInt(value: Int32) {
        let encoded = Varint(fromValue: Int64(value))
        bytes += encoded.backing
        return
    }

    func encodeLong(value: Int64) {
        let encoded = Varint(fromValue: UInt64(value))
        bytes += encoded.backing
        return
    }
    
    func encodeFloat(value: Float) {
        let bits: UInt64 = unsafeBitCast(value, UInt64.self)

        bytes.append(Byte(0xff & (bits >> 24)))
        bytes.append(Byte(0xff & (bits >> 16)))
        bytes.append(Byte(0xff & (bits >> 8)))
        bytes.append(Byte(0xff & bits))
        return
    }
    
    func encodeDouble(value: Double) {
        let bits: UInt64 = unsafeBitCast(value, UInt64.self)

        bytes.append(Byte(0xff & (bits >> 56)))
        bytes.append(Byte(0xff & (bits >> 48)))
        bytes.append(Byte(0xff & (bits >> 40)))
        bytes.append(Byte(0xff & (bits >> 32)))
        bytes.append(Byte(0xff & (bits >> 24)))
        bytes.append(Byte(0xff & (bits >> 16)))
        bytes.append(Byte(0xff & (bits >> 8)))
        bytes.append(Byte(0xff & bits))
        return
    }

    func encodeString(value: String) {
        var stringBytes: [Byte] = []
        for char in value.utf8 {
            stringBytes.append(char)
        }
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
    
}
