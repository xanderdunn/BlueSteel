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

    public func decodeDouble() -> Double {
        var bitpattern = self.decodeLong()

        var result = withUnsafePointer(&bitpattern, { (ptr: UnsafePointer<Int64>) -> Double in
            return UnsafePointer<Double>(ptr).memory
        })

        return result
    }


    public func decodeFloat() -> Float {
        var bitpattern = self.decodeInt()

        var result = withUnsafePointer(&bitpattern, { (ptr: UnsafePointer<Int32>) -> Float in
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
