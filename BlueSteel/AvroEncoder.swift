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
    var bytes: [Byte] = []
    var schema: Schema

    public var byteArray: [Byte] {
        return self.bytes
    }

    public var data: NSData {
        return NSData(bytes: &self.bytes, length: self.bytes.count)
    }

    func emitNull() -> Result<(), NSError> {
        return success(())
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

    public func emitValue(value: AvroValue) -> Result<(), NSError> {
        switch schema {

        case .AvroNullSchema :
            self.emitNull()

        case .AvroBooleanSchema :
            switch value {
            case .AvroBooleanValue(let value) :
                self.emitBool(value)
            default :
                return failure("Schema expected Boolean value.")
            }

        case .AvroIntSchema :
            switch value {
            case .AvroIntValue(let value) :
                self.emitInt32(value)
            default :
                return failure("Schema expected Int value.")
            }

        case .AvroLongSchema :
            switch value {
            case .AvroIntValue(let value) :
                self.emitInt64(Int64(value))
            case .AvroLongValue(let value) :
                self.emitInt64(value)
            default :
                return failure("Schema expected Long value.")
            }

        case .AvroFloatSchema :
            switch value {
            case .AvroIntValue(let value) :
                self.emitFloat(Float(value))
            case .AvroLongValue(let value) :
                self.emitFloat(Float(value))
            case .AvroFloatValue(let value) :
                self.emitFloat(value)
            default :
                return failure("Schema expected Float value.")
            }

        case .AvroDoubleSchema :
            switch value {
            case .AvroIntValue(let value) :
                self.emitDouble(Double(value))
            case .AvroLongValue(let value) :
                self.emitDouble(Double(value))
            case .AvroFloatValue(let value) :
                self.emitDouble(Double(value))
            case .AvroDoubleValue(let value) :
                self.emitDouble(value)
            default :
                return failure("Schema expected Double value.")
            }

        case .AvroStringSchema, .AvroBytesSchema :
            switch value {
            case .AvroStringValue(let value) :
                self.emitString(value)
            case .AvroBytesValue(let value) :
                self.emitBytes(value)
            default :
                return failure("Schema expected String value.")
            }

        case .AvroArraySchema(let box) :
            switch value {
            case .AvroArrayValue(let values) :
                if values.count != 0 {
                    let subEncoder = AvroEncoder(schema: box.value)
                    subEncoder.emitInt64(Int64(values.count))

                    values.reduce(success(()), combine: { (res, val) -> Result<(), NSError> in
                        if res.isSuccess {
                            return subEncoder.emitValue(val)
                        }
                        return res
                    })
                    bytes += subEncoder.bytes
                }
                self.emitInt64(0)
            default :
                return failure("Schema expected Array value.")
            }

        case .AvroMapSchema(let box) :
            switch value {
            case .AvroMapValue(let pairs) :
                if pairs.count != 0 {
                    let subEncoder = AvroEncoder(schema: box.value)
                    subEncoder.emitInt64(Int64(pairs.count))

                    for (key, value) in pairs {
                        subEncoder.emitString(key)
                        let res = subEncoder.emitValue(value)
                        if !res.isSuccess {
                            return res
                        }
                    }
                    bytes += subEncoder.bytes
                }
                self.emitInt64(0)
            default :
                return failure("Schema expected Map value.")
            }

        case .AvroRecordSchema(let name, let fieldSchemas) :
            switch value {
            case .AvroRecordValue(let pairs) :
                for fSchema in fieldSchemas {
                    switch fSchema {
                    case .AvroFieldSchema(let key, let box) :
                        let subEncoder = AvroEncoder(schema: box.value)

                        if let value = pairs[key] {
                            let res = subEncoder.emitValue(value)
                            if !res.isSuccess {
                                return res
                            }
                        } else {
                            // Since we don't support schema defaults, fail encoding when values are missing for schema keys.
                            return failure("Missing value for key \(key)")
                        }
                        bytes += subEncoder.bytes
                    default :
                        return failure("Expected field schema.")
                    }
                }
            default :
                return failure("Schema expected record value.")
            }

        case .AvroEnumSchema(let name, let enumSchemas) :
            switch value {
                //TODO: Make sure enum matches schema
            case .AvroEnumValue(let index, _) :
                self.emitInt32(Int32(index))
            default :
                return failure("Schema expected Enum value.")
            }

        case .AvroUnionSchema(let uSchemas) :
            switch value {
            case .AvroUnionValue(let index, let box) :
                self.emitInt64(Int64(index))
                if index < uSchemas.count {
                    let subEncoder = AvroEncoder(schema: uSchemas[index])
                    let res = subEncoder.emitValue(box.value)

                    bytes += subEncoder.bytes
                } else {
                    return failure("Union value index out of schema bounds.")
                }
            default :
                return failure("Schema expected Union value.")
            }


        case .AvroFixedSchema(_, let size) :
            switch value {
            case .AvroFixedValue(let fixedBytes) :
                if fixedBytes.count == size {
                    self.emitFixed(fixedBytes)
                } else {
                    return failure("Fixed value size: \(fixedBytes.count). Expected: \(size)")
                }
            default :
                return failure("Schema expected Fixed value.")
            }

        case .AvroFieldSchema(_) :
            return failure("Field Schemas should be handled as part of a record value.")
        case .AvroInvalidSchema :
            return failure("Invalid schema.")
        }

        return success(())
    }

    public init(schema: Schema) {
        bytes = []
        self.schema = schema
    }
}
