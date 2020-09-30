//
//  AvroStreamDecoder.swift
//  BlueSteel
//
//  Created by Stefan Paychère.
//  Copyright © 2019 Myotest. All rights reserved.
//

import Foundation

extension Varint {
    public static func VarintFromInputStream(_ inputStream: InputStream) -> Varint? {
        var capacity = 8
        var buf = UnsafeMutablePointer<UInt8>.allocate(capacity: capacity)
        var ptr = buf
        var count = 0
        
        while inputStream.read(ptr, maxLength: 1) == 1 {
            count += 1
            if (ptr.pointee & 0x80) == 0 {
                break
            }
            if (count >= capacity) && inputStream.hasBytesAvailable { // handle buffer overflow
                let source = buf
                buf = UnsafeMutablePointer<UInt8>.allocate(capacity: capacity + 8)
                buf.assign(from: source, count: capacity)
                source.deinitialize(count: capacity)
                source.deallocate()
                ptr = buf.advanced(by: capacity)
                capacity = capacity + 8
            } else {
                ptr += 1
            }
        }
        
        if count > 0 {
            var bytes = [UInt8](repeating: 0, count: count)
            for index in 0...count-1 {
                bytes[index] = buf[index]
            }
            //return Varint(fromBytes: bytes)
            return VarintFromBytes(bytes)
        }
        
        buf.deinitialize(count: capacity)
        buf.deallocate()
        
        return nil
    }
}

open class AvroStreamDecoder: AvroDecoder {
    var dataBytes: [UInt8]?
    var inputStream: InputStream?
    
    public init(_ inputStream:InputStream) {
        super.init([])
        self.inputStream = inputStream
    }
    
    override public init(_ data:[UInt8]) {
        super.init(data)
        dataBytes = data
    }
    
    private func getBytes(_ count: Int) -> [UInt8]? {
        if let inputStream = inputStream {
            var buffer = [UInt8](repeating: 0, count: count)
            if  inputStream.read(&buffer, maxLength: count) == count {
                return  buffer
            }
        } else if dataBytes != nil {
            if dataBytes!.count >= count {
                let bytes = [UInt8](dataBytes!.prefix(count))
                dataBytes!.removeSubrange(0...count-1)
                return bytes
            }
        }
        return nil
    }
    
    private func getVarInt() -> Varint? {
        if let inputStream = inputStream {
            return Varint.VarintFromInputStream(inputStream)
        } else if let bytes = dataBytes {
            if let varint = Varint.VarintFromBytes(bytes) {
                dataBytes!.removeSubrange(0...varint.count - 1)
                return varint
            }
        }
        return nil
    }
    
    override open func decodeBoolean() -> Bool? {
        guard let bytes = getBytes(1) else {
            return nil
        }
        
        let result: Bool = bytes[0] > 0
        return result
    }
    
    override open func decodeDouble() -> Double? {
        guard let slice = getBytes(8) else {
            return nil
        }
        
        var bits: UInt64 = UInt64(slice[slice.startIndex])
        bits |= UInt64(slice[slice.startIndex + 1]) << 8
        bits |= UInt64(slice[slice.startIndex + 2]) << 16
        bits |= UInt64(slice[slice.startIndex + 3]) << 24
        bits |= UInt64(slice[slice.startIndex + 4]) << 32
        bits |= UInt64(slice[slice.startIndex + 5]) << 40
        bits |= UInt64(slice[slice.startIndex + 6]) << 48
        bits |= UInt64(slice[slice.startIndex + 7]) << 56
        
        let result = withUnsafePointer(to: &bits, { (ptr: UnsafePointer<UInt64>) -> Double in
            return ptr.withMemoryRebound(to: Double.self, capacity: 1) { memory in
                return memory.pointee
            }
        })
        return result
    }

    override open func decodeFloat() -> Float? {
        guard let slice = getBytes(4) else {
            return nil
        }
        
        var bits: UInt32 = UInt32(slice[slice.startIndex])
        bits |= UInt32(slice[slice.startIndex + 1]) << 8
        bits |= UInt32(slice[slice.startIndex + 2]) << 16
        bits |= UInt32(slice[slice.startIndex + 3]) << 24
        
        let result = withUnsafePointer(to: &bits, { (ptr: UnsafePointer<UInt32>) -> Float in
            return ptr.withMemoryRebound(to: Float.self, capacity: 1) { return $0.pointee }
        })
        return result
    }

    override open func decodeInt() -> Int32? {
        if let x = getVarInt() {
            return Int32(x.toUInt64().decodeZigZag())
        }
        return nil
    }
    
    override open func decodeLong() -> Int64? {
        if let x = getVarInt() {
            return Int64(x.toUInt64().decodeZigZag())
        }
        return nil
    }
    
    override open func decodeBytes() -> [UInt8]? {
        if let sizeLong = decodeLong() {
            let size = Int(sizeLong)
            return getBytes(size)
        }
        return nil
    }
    
    override open func decodeFixed(_ size: Int) -> [UInt8]? {
        return getBytes(size)
    }
    
}
