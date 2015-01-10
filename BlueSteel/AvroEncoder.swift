//
//  AvroEncoder.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation
import LlamaKit

public class AvroEncoder {

    // Backing
    var bytes: [Byte] = []

    func emitNull() {
        return
    }

    func emitBool(value: Bool) -> Result<(), NSError> {
        if value {
            bytes.append(Byte(0x1))
        } else {
            bytes.append(Byte(0x0))
        }
        return success(())
    }

    func emitInt32(value: Int32) -> Result<(), NSError> {
        let encoded = Varint(fromValue: Int64(value).encodeZigZag())
        bytes += encoded.backing
        return success(())
    }

    func emitInt64(value: Int64) -> Result<(), NSError> {
        let encoded = Varint(fromValue: value.encodeZigZag())
        bytes += encoded.backing
        return success(())
    }
    
    func emitFloat(value: Float) -> Result<(), NSError> {
        let bits: UInt32 = unsafeBitCast(value, UInt32.self)

        let encodedFloat = [Byte(0xff & bits),
            Byte(0xff & (bits >> 8)),
            Byte(0xff & (bits >> 16)),
            Byte(0xff & (bits >> 24))]

        bytes += encodedFloat
        return success(())
    }
    
    func emitDouble(value: Double) -> Result<(), NSError> {
        let bits: UInt64 = unsafeBitCast(value, UInt64.self)

        let encodedDouble = [Byte(0xff & bits),
            Byte(0xff & (bits >> 8)),
            Byte(0xff & (bits >> 16)),
            Byte(0xff & (bits >> 24)),
            Byte(0xff & (bits >> 32)),
            Byte(0xff & (bits >> 40)),
            Byte(0xff & (bits >> 48)),
            Byte(0xff & (bits >> 56))]
        bytes += encodedDouble
        return success(())
    }

    func emitString(value: String) -> Result<(), NSError> {
        var cstr = value.cStringUsingEncoding(NSUTF8StringEncoding)!
        let bufferptr = UnsafeBufferPointer<Byte>(start: UnsafePointer<Byte>(cstr), count: cstr.count - 1)

        let stringBytes = [Byte](bufferptr)
        emitBytes(stringBytes)
        return success(())
    }

    func emitBytes(value: [Byte]) -> Result<(), NSError> {
        emitInt64(Int64(value.count))
        bytes += value
        return success(())
    }

    func emitFixed(value: [Byte]) -> Result<(), NSError> {
        bytes += value
        return success(())
    }

    public init() {
        bytes = []
    }
}
