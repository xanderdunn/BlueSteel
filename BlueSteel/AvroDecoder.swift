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

    public func decodeNull() {
        // Nulls aren't actually encoded.
        return
    }

    public func decodeBoolean() -> Bool? {
        if (bytes.count == 0) {
            return nil
        }

        let result: Bool = bytes[0] > 0
        bytes.removeAtIndex(0)

        return result
    }

    public func decodeDouble() -> Double? {
        if (bytes.count < 8) {
            return nil
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

        bytes.removeRange(0...7)

        let result = withUnsafePointer(&bits, { (ptr: UnsafePointer<UInt64>) -> Double in
            return UnsafePointer<Double>(ptr).memory
        })
        return result
    }


    public func decodeFloat() -> Float? {

        if (bytes.count < 4) {
            return nil
        }

        let slice = bytes[0...3]
        var bits: UInt32 = UInt32(slice[0]) << 24 |
            UInt32(slice[1]) << 16 |
            UInt32(slice[2]) << 8 |
            UInt32(slice[3])

        bytes.removeRange(0...3)

        let result = withUnsafePointer(&bits, { (ptr: UnsafePointer<UInt32>) -> Float in
            return UnsafePointer<Float>(ptr).memory
        })
        return result
    }

    public func decodeInt() -> Int32? {
        switch Varint.VarintFromBytes(bytes) {
            case let .Some(x):
                bytes.removeRange(0...x.count - 1)
                return Int32(x.toUInt().decodeZigZag())
            case .None:
                return nil
        }
    }

    public func decodeLong() -> Int64? {
        switch Varint.VarintFromBytes(bytes) {
            case let .Some(x):
                bytes.removeRange(0...x.count - 1)
                return Int64(x.toUInt().decodeZigZag())
            case .None:
                return nil
        }
    }

    // Avro doesnt actually support Unsigned primitives. So We'll keep this internal.
    internal func decodeUInt() -> UInt {
        // Stub
        return 0.0
    }

    public func decodeBytes() -> [Byte]? {
        let size = decodeLong()

        switch size {
            case let .Some(x):
                if (Int64(bytes.count) < size) {
                    return nil
                }

                var tmp: [Byte] = [Byte](bytes[0...x - 1])
                bytes.removeRange(0...x - 1)
                return tmp

            case .None:
                return nil
        }
    }

    public func decodeString() -> String? {

        if let rawString = decodeBytes()? {
            return String.stringWithBytes(rawString, encoding: NSUTF8StringEncoding)
        } else {
            return nil
        }
    }

}
