//
//  AvroDecoder.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import LlamaKit

extension NSData {
    public var byteArray: [Byte] {
        let dataPointer = UnsafePointer<Byte>(self.bytes)
        let bufferPointer = UnsafeBufferPointer<Byte>(start: dataPointer, count: self.length)
        return [Byte](bufferPointer)
    }
}

// TODO: Make this thread safe.

public class AvroDecoder {
    var bytes: [Byte] = []
    let schema: Schema

    public init(schema: Schema, data: [Byte]) {
        self.schema = schema
        self.bytes = data
    }

    public func decodeNull() {
        // Nulls aren't actually encoded.
        return
    }

    public func decodeBoolean() -> Result<Bool, NSError> {
        if (bytes.count == 0) {
            return failure("Byte buffer empty.")
        }

        let result: Bool = bytes[0] > 0
        bytes.removeAtIndex(0)

        return success(result)
    }

    public func decodeDouble() -> Result<Double, NSError> {
        if (bytes.count < 8) {
            return failure("Insufficient data in buffer.")
        }

        let slice = bytes[0...7]

        var bits: UInt64 = UInt64(slice[0])
            bits |= UInt64(slice[1]) << 8
            bits |= UInt64(slice[2]) << 16
            bits |= UInt64(slice[3]) << 24
            bits |= UInt64(slice[4]) << 32
            bits |= UInt64(slice[5]) << 40
            bits |= UInt64(slice[6]) << 48
            bits |= UInt64(slice[7]) << 56

        bytes.removeRange(0...7)

        let result = withUnsafePointer(&bits, { (ptr: UnsafePointer<UInt64>) -> Double in
            return UnsafePointer<Double>(ptr).memory
        })
        return success(result)
    }

    public func decodeFloat() -> Result<Float, NSError> {

        if (bytes.count < 4) {
            return failure("Insufficient data in buffer.")
        }

        let slice = bytes[0...3]
        var bits: UInt32 = UInt32(slice[0])
            bits |= UInt32(slice[1]) << 8
            bits |= UInt32(slice[2]) << 16
            bits |= UInt32(slice[3]) << 24

        bytes.removeRange(0...3)

        let result = withUnsafePointer(&bits, { (ptr: UnsafePointer<UInt32>) -> Float in
            return UnsafePointer<Float>(ptr).memory
        })
        return success(result)
    }

    public func decodeInt32() -> Result<Int32, NSError> {
        if let x = Varint.VarintFromBytes(bytes) {
            if x.count > 0 {
                bytes.removeRange(0...x.count - 1)
                return success(Int32(x.toUInt64().decodeZigZag()))
            }
        }
        return failure("Couldn't obtain varint from buffer.")
    }

    public func decodeInt64() -> Result<Int64, NSError> {
        if let x = Varint.VarintFromBytes(bytes) {
            if x.count > 0 {
                bytes.removeRange(0...x.count - 1)
                return success(Int64(x.toUInt64().decodeZigZag()))
            }
        }
        return failure("Couldn't obtain varint from buffer.")
    }

    public func decodeBytes() -> Result<[Byte], NSError> {
        return decodeInt64().flatMap { size -> Result<[Byte], NSError> in
            if size <= Int64(self.bytes.count) && size != 0 {
                var tmp: [Byte] = [Byte](self.bytes[0...size - 1])
                self.bytes.removeRange(0...size - 1)
                return success(tmp)
            }
            return failure("Insufficient data in buffer.")
        }
    }

    public func decodeString() -> Result<String, NSError> {
        return decodeBytes().flatMap { rawString -> Result<String, NSError> in
            if let result =  NSString(bytes: rawString, length: rawString.count, encoding: NSUTF8StringEncoding) {
                return success(result)
            }
            return failure("Coudln't convert buffer to UTF-8 String.")
        }
    }

    public func decodeFixed(size: Int) -> Result<[Byte], NSError> {
        if bytes.count < size {
            return failure("Insufficient data in buffer.")
        }
        let tmp: [Byte] = [Byte](bytes[0...size - 1])
        bytes.removeRange(0...size - 1)
        return success(tmp)
    }

    public func decodeValue() -> Result<AvroValue, NSError> {
        switch schema {
        case .AvroNullSchema :
            return success(.AvroNullValue)

        case .AvroBooleanSchema :
            return self.decodeBoolean().map { decoded in
                return .AvroBooleanValue(decoded)
            }

        case .AvroIntSchema :
            return self.decodeInt32().map { decoded in
                .AvroIntValue(decoded)
            }

        case .AvroLongSchema :
            return self.decodeInt64().map { decoded in
                .AvroLongValue(decoded)
            }

        case .AvroFloatSchema :
            return self.decodeFloat().map { decoded in
                .AvroFloatValue(decoded)
            }

        case .AvroDoubleSchema :
            return self.decodeDouble().map { decoded in
                .AvroDoubleValue(decoded)
            }

        case .AvroStringSchema :
            return self.decodeString().map { decoded in
                .AvroStringValue(decoded)
            }

        case .AvroBytesSchema :
            return self.decodeBytes().map { decoded in
                .AvroBytesValue(decoded)
            }

        // TODO: Collections negative count support.
        case .AvroArraySchema(let boxedSchema) :
            return self.decodeInt64().flatMap { size -> Result<AvroValue, NSError> in
                if size == 0 {
                    return success(.AvroArrayValue([]))
                }

                let subDecoder = AvroDecoder(schema: boxedSchema.value, data: self.bytes)

                var values = Array<AvroValue>(count: Int(size), repeatedValue: .AvroInvalidValue)
                //var res = Array<Result<AvroValue, NSError>>(count: Int(size), repeatedValue: success(.AvroInvalidValue))

                for (index, _) in enumerate(values) {
                    let subvalue = subDecoder.decodeValue()
                    if !subvalue.isSuccess {
                        return subvalue
                    }
                    values[index] = subvalue.value!
                }
                self.bytes = subDecoder.bytes

                return success(.AvroArrayValue(values))
            }

        case .AvroMapSchema(let boxedSchema) :

            return self.decodeInt64().flatMap { size -> Result<AvroValue, NSError> in
                if size == 0 {
                    return success(.AvroMapValue([:]))
                }

                let subDecoder = AvroDecoder(schema: boxedSchema.value, data: self.bytes)

                var values = Array<AvroValue>(count: Int(size), repeatedValue: .AvroInvalidValue)
                var pairs = Dictionary<String, AvroValue>()

                for (index, _) in enumerate(values) {
                    let kv = subDecoder.decodeString().flatMap { key -> Result<AvroValue, NSError> in
                        return subDecoder.decodeValue().map { value in
                            pairs[key] = value
                            return value
                        }
                    }
                    if !kv.isSuccess {
                        return kv
                    }
                }
                self.bytes = subDecoder.bytes

                return success(.AvroMapValue(pairs))
            }

        case .AvroEnumSchema(_, let enumValues) :
            return self.decodeInt32().flatMap { index -> Result<AvroValue, NSError> in
                let intIndex = Int(index)
                if intIndex > enumValues.count - 1 {
                    return failure("Enum index out of bounds.")
                }
                return success(.AvroEnumValue(intIndex, enumValues[intIndex]))
            }

        case .AvroRecordSchema(_, let fields) :
            var pairs: Dictionary<String, AvroValue> = [:]
            for field in fields {
                switch field {
                case .AvroFieldSchema(let key, let boxedSchema) :
                    let subDecoder = AvroDecoder(schema: boxedSchema.value, data: self.bytes)
                    let subValue = subDecoder.decodeValue().map { value -> AvroValue in
                        pairs[key] = value
                        return value
                    }
                    if !subValue.isSuccess {
                        return subValue
                    }
                    self.bytes = subDecoder.bytes
                default :
                    return failure("Expected field schema.")
                }
            }
            return  success(.AvroRecordValue(pairs))

        case .AvroFixedSchema(_, let size) :
            return self.decodeFixed(size).map { value in
                return .AvroFixedValue(value)
            }

        case .AvroUnionSchema(let schemas) :

            return self.decodeInt64().flatMap { index in
                let intIndex = Int(index)

                if intIndex < schemas.count {
                    let subDecoder = AvroDecoder(schema: schemas[intIndex], data: self.bytes)
                    let subValue = subDecoder.decodeValue()

                    self.bytes = subDecoder.bytes
                    return subValue
                }
                return failure("Union index out of bounds")
            }

        default :
            return failure("Unhandled schema type")
        }
    }
}
