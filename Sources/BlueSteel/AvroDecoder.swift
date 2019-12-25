//
//  AvroDecoder.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

// TODO: Make this thread safe.

open class AvroDecoder {
    var bytes: [UInt8] = []

    public init(_ data:Data) {
        let dataPointer = (data as NSData).bytes.bindMemory(to: UInt8.self, capacity: data.count)
        let bufferPointer = UnsafeBufferPointer<UInt8>(start: dataPointer, count: data.count)
        bytes = [UInt8](bufferPointer)
    }

    public init(_ data:[UInt8]) {
        bytes = data
    }

    open func decodeNull() {
        // Nulls aren't actually encoded.
        return
    }

    open func decodeBoolean() -> Bool? {
        if (bytes.count == 0) {
            return nil
        }

        let result: Bool = bytes[0] > 0
        bytes.remove(at: 0)

        return result
    }

    open func decodeDouble() -> Double? {
        if (bytes.count < 8) {
            return nil
        }

        let slice = bytes[0...7]

        var bits: UInt64 = UInt64(slice[slice.startIndex])
            bits |= UInt64(slice[slice.startIndex + 1]) << 8
            bits |= UInt64(slice[slice.startIndex + 2]) << 16
            bits |= UInt64(slice[slice.startIndex + 3]) << 24
            bits |= UInt64(slice[slice.startIndex + 4]) << 32
            bits |= UInt64(slice[slice.startIndex + 5]) << 40
            bits |= UInt64(slice[slice.startIndex + 6]) << 48
            bits |= UInt64(slice[slice.startIndex + 7]) << 56

        bytes.removeSubrange(0...7)

        let result = withUnsafePointer(to: &bits, { (ptr: UnsafePointer<UInt64>) -> Double in
            return ptr.withMemoryRebound(to: Double.self, capacity: 1) { memory in
                return memory.pointee
            }
        })
        return result
    }


    open func decodeFloat() -> Float? {

        if (bytes.count < 4) {
            return nil
        }

        let slice = bytes[0...3]
        var bits: UInt32 = UInt32(slice[slice.startIndex])
            bits |= UInt32(slice[slice.startIndex + 1]) << 8
            bits |= UInt32(slice[slice.startIndex + 2]) << 16
            bits |= UInt32(slice[slice.startIndex + 3]) << 24

        bytes.removeSubrange(0...3)

        let result = withUnsafePointer(to: &bits, { (ptr: UnsafePointer<UInt32>) -> Float in
            return ptr.withMemoryRebound(to: Float.self, capacity: 1) { return $0.pointee }
        })
        return result
    }

    open func decodeInt() -> Int32? {
        if let x = Varint.VarintFromBytes(bytes) {
            if x.count > 0 {
                bytes.removeSubrange(0...x.count - 1)
                return Int32(x.toUInt64().decodeZigZag())
            }
        }
        return nil
    }

    open func decodeLong() -> Int64? {
        if let x = Varint.VarintFromBytes(bytes) {
            if x.count > 0 {
                bytes.removeSubrange(0...x.count - 1)
                return Int64(x.toUInt64().decodeZigZag())
            }
        }
        return nil
    }

    // Avro doesnt actually support Unsigned primitives. So We'll keep this internal.
    internal func decodeUInt() -> UInt {
        // Stub
        return 0
    }

    open func decodeBytes() -> [UInt8]? {
        if let sizeLong = decodeLong() {
            let size = Int(sizeLong)
            if size <= Int(bytes.count) && size != 0 {
                let tmp = bytes[0..<size]
                bytes.removeSubrange(0..<size)
                return [UInt8](tmp)
            }
        }
        return nil
    }

    open func decodeString() -> String? {
        if let rawString = decodeBytes() {
            //return String.stringWithBytes(rawString, encoding: NSUTF8StringEncoding)
            //let result: String? = NSString(bytes: rawString, length: rawString.count, encoding: NSUTF8StringEncoding)
            let result = String(bytes: rawString, encoding: String.Encoding.utf8)
            return result
        } else {
            return nil
        }
    }

    open func decodeFixed(_ size: Int) -> [UInt8]? {
        if bytes.count < size {
            return nil
        }
        let tmp: [UInt8] = [UInt8](bytes[0...size - 1])
        bytes.removeSubrange(0...size - 1)
        return tmp
    }
}
