//
//  AvroDecoder.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

// TODO: Make this thread safe.

public class AvroDecoder {
    var bytes: [Byte] = []

    public init(_ data:NSData) {
        let dataPointer = UnsafePointer<Byte>(data.bytes)
        let bufferPointer = UnsafeBufferPointer<Byte>(start: dataPointer, count: data.length)
        bytes = [Byte](bufferPointer)
    }

    public init(_ data:[Byte]) {
        bytes = data
    }

    public func decodeDouble() -> Double? {
        if (bytes.count < 8) {
            return .None
        }

        let slice = bytes[0...7]
        var bits: UInt64 = UInt64(slice[0]) << 56 |
            UInt64(slice[1]) << 48 |
            UInt64(slice[2]) << 40 |
            UInt64(slice[3]) << 32 |
            UInt64(slice[4]) << 24 |
            UInt64(slice[5]) << 16 |
            UInt64(slice[6]) << 8 |
            UInt64(slice[7])

        let result = withUnsafePointer(&bits, { (ptr: UnsafePointer<UInt64>) -> Double in
            return UnsafePointer<Double>(ptr).memory
        })
        return result
    }


    public func decodeFloat() -> Float? {

        if (bytes.count < 4) {
            return .None
        }

        let slice = bytes[0...3]
        var bits: UInt32 = UInt32(slice[0]) << 24 |
            UInt32(slice[1]) << 16 |
            UInt32(slice[2]) << 8 |
            UInt32(slice[3])

        let result = withUnsafePointer(&bits, { (ptr: UnsafePointer<UInt32>) -> Float in
            return UnsafePointer<Float>(ptr).memory
        })
        return result
    }

    public func decodeInt() -> Int32 {
        switch Varint.VarintFromBytes(bytes) {
            case let .Some(x):
                bytes.removeRange(0...x.count - 1)
                return Int32(x.toUInt().decodeZigZag())
            case .None:
                // TODO: Error case. Should return something better than just "0".
                return 0
        }
    }

    public func decodeLong() -> Int64 {
        switch Varint.VarintFromBytes(bytes) {
            case let .Some(x):
                bytes.removeRange(0...x.count - 1)
                return Int64(x.toUInt().decodeZigZag())
            case .None:
                // TODO: Error case. Should return something better than just "0".
                return 0
        }
    }

    func decodeUInt() -> UInt {
        // Stub
        return 0.0
    }

    func decodeBytes() -> [UInt8]? {
        // Stub
        return nil
    }

    func decodeString() -> String? {
        // Stub
        return nil
    }

}
