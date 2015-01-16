//
//  AvroValueTests.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import UIKit
import XCTest
import LlamaKit
import BlueSteel

class AvroDecoderTestCase: XCTestCase {

    override func setUp() {
        super.setUp()
    }

    override func tearDown() {
        super.tearDown()
    }

    func testStringValue() {
        var avroBytes: [Byte] = [0x06, 0x66, 0x6f, 0x6f]
        let jsonSchema = "{ \"type\" : \"string\" }"

        if let error = AvroDecoder(schema: Schema(string: jsonSchema), data: avroBytes)
            .decodeValue()
            .map({ avroString -> () in
                if let string = avroString.string {
                    XCTAssertEqual(string, "foo", "Strings don't match.")
                }
            }).error {
                XCTFail("Value decoding failed with errror: \(error)")
        }
    }

    func testByteValue() {
        var avroBytes: [Byte] = [0x06, 0x66, 0x6f, 0x6f]
        let jsonSchema = "{ \"type\" : \"bytes\" }"

        if let error = AvroDecoder(schema: Schema(string: jsonSchema), data: avroBytes)
            .decodeValue()
            .map({ avroBytes -> () in
                if let bytes = avroBytes.bytes {
                    XCTAssertEqual(bytes, [0x66, 0x6f, 0x6f], "Byte arrays don't match.")
                }
            }).error {
                XCTFail("Value decoding failed with errror: \(error)")
        }
    }

    func testIntValue() {
        let avroBytes: [Byte] = [0x96, 0xde, 0x87, 0x3]
        let jsonSchema = "{ \"type\" : \"int\" }"

        if let error = AvroDecoder(schema: Schema(string: jsonSchema), data: avroBytes)
            .decodeValue()
            .map({ avroInt -> () in
                if let integer = avroInt.integer {
                    XCTAssertEqual(Int(integer), 3209099, "Unexpected value.")
                }
            }).error {
                XCTFail("Value decoding failed with errror: \(error)")
        }
    }

    func testLongValue() {
        let avroBytes: [Byte] = [0x96, 0xde, 0x87, 0x3]
        let jsonSchema = "{ \"type\" : \"long\" }"

        if let error = AvroDecoder(schema: Schema(string: jsonSchema), data: avroBytes)
            .decodeValue()
            .map({ avroLong -> () in
                if let long = avroLong.long {
                    XCTAssertEqual(Int(long), 3209099, "Unexpected value.")
                }
            }).error {
                XCTFail("Value decoding failed with errror: \(error)")
        }
    }

    func testFloatValue() {
        let avroBytes: [Byte] = [0xc3, 0xf5, 0x48, 0x40]
        let jsonSchema = "{ \"type\" : \"float\" }"
        let expected: Float = 3.14

        if let error = AvroDecoder(schema: Schema(string: jsonSchema), data: avroBytes)
            .decodeValue()
            .map({ avroFloat -> () in
                if let float = avroFloat.float {
                    XCTAssertEqual(float, expected, "Unexpected value.")
                }
            }).error {
                XCTFail("Value decoding failed with errror: \(error)")
        }
    }

    func testDoubleValue() {
        let avroBytes: [Byte] = [0x1f, 0x85, 0xeb, 0x51, 0xb8, 0x1e, 0x9, 0x40]
        let jsonSchema = "{ \"type\" : \"double\" }"
        let expected: Double = 3.14

        if let error = AvroDecoder(schema: Schema(string: jsonSchema), data: avroBytes)
            .decodeValue()
            .map({ avroDouble -> () in
                if let double = avroDouble.double {
                    XCTAssertEqual(double, expected, "Unexpected value.")
                }
            }).error {
                XCTFail("Value decoding failed with errror: \(error)")
        }
    }

    func testBooleanValue() {
        var avroFalseBytes: [Byte] = [0x0]
        var avroTrueBytes: [Byte] = [0x1]

        let schema = Schema(string: "{ \"type\" : \"boolean\" }")

        let trueResult = AvroDecoder(schema: schema, data: avroTrueBytes)
            .decodeValue().map { avroValue -> AvroValue in
                if let value = avroValue.boolean {
                    XCTAssert(value, "Value should be true.")
                } else {
                    XCTFail("Value should be boolean.")
                }
                return avroValue
            }

        if !trueResult.isSuccess {
            XCTFail("Decoding error: \(trueResult.error!)")
        }

        let falseResult = AvroDecoder(schema: schema, data: avroFalseBytes)
            .decodeValue().map { avroValue -> AvroValue in
                if let value = avroValue.boolean {
                    XCTAssert(!value, "Value should be false.")
                } else {
                    XCTFail("Value should be boolean.")
                }
                return avroValue
        }

        if !falseResult.isSuccess {
            XCTFail("Decoding error: \(falseResult.error!)")
        }
    }

    func testArrayValue() {
        let avroBytes: [Byte] = [0x04, 0x06, 0x36, 0x00]
        let expected: [Int64] = [3, 27]
        let jsonSchema = "{ \"type\" : \"array\", \"items\" : \"long\" }"


        if let error = AvroDecoder(schema: Schema(string: jsonSchema), data: avroBytes)
            .decodeValue()
            .map({ avroArray -> () in
                if let array = avroArray.array {
                    XCTAssertEqual(array.count, 2, "Wrong number of elements in array.")

                    for (index, value) in enumerate(array) {
                        XCTAssertEqual(value.long!, expected[index], "Unexpected value.")
                    }
                } else {
                    XCTFail("Expected array value")
                }
            }).error {
                XCTFail("Value decoding failed with errror: \(error)")
        }
    }

    func testMapValue() {
        let avroBytes: [Byte] = [0x02, 0x06, 0x66, 0x6f, 0x6f, 0x36, 0x00]
        let expected: [Int64] = [27]
        let jsonSchema = "{ \"type\" : \"map\", \"values\" : \"long\" }"

        if let error = AvroDecoder(schema: Schema(string: jsonSchema), data: avroBytes)
            .decodeValue()
            .map({ avroMap -> () in
                if let dict = avroMap.map {
                    XCTAssertEqual(dict.count, 1, "Wrong number of elements in array.")

                    for (key, value) in dict {
                        XCTAssertEqual(value.long!, expected[0], "Unexpected value.")
                    }
                } else {
                    XCTFail("Expected map value")
                }
            }).error {
                XCTFail("Value decoding failed with errror: \(error)")
        }
    }

    func testUnionValue() {
        let avroBytes: [Byte] = [0x02, 0x02, 0x61]
        let jsonSchema = "{\"type\" : [\"null\",\"string\"] }"

        if let error = AvroDecoder(schema: Schema(string: jsonSchema), data: avroBytes)
            .decodeValue()
            .map({ avroUnion -> () in
                if let string = avroUnion.string {
                    XCTAssertEqual(string, "a", "Unexpected value.")
                } else {
                    XCTFail("Expected string from union value.")
                }
            }).error {
                XCTFail("Value decoding failed with errror: \(error)")
        }
    }
    func testPerformanceStub() {
        self.measureBlock() {
        }
    }
}

class AvroValueEquatableTestCase: XCTestCase {

    override func setUp() {
        super.setUp()
    }

    override func tearDown() {
        super.tearDown()
    }

    // Helpers
    // Random byte.
    var rbyte: Byte {
        return Byte(arc4random() & 0xff)
    }

    // Random byte array
    var rbyteArray: [Byte] {
        let size = Int(rbyte)
        var array = Array<Byte>(count: size, repeatedValue: 0)

        for idx in 0...size - 1 {
            array[idx] = rbyte
        }
        return array
    }


    func testNullEquality() {
        XCTAssert(AvroValue.AvroNullValue == AvroValue.AvroNullValue, "Null values should be equal.")
        XCTAssert(AvroValue.AvroNullValue != AvroValue.AvroInvalidValue, "Null values shouldn't be equal to values of another type.")
    }

    func testBoolEquality() {
        XCTAssert(AvroValue.AvroBooleanValue(true) == AvroValue.AvroBooleanValue(true), "Boolean values should be equal.")
        XCTAssert(AvroValue.AvroBooleanValue(false) == AvroValue.AvroBooleanValue(false), "Boolean values should be equal.")
        XCTAssert(AvroValue.AvroBooleanValue(true) != AvroValue.AvroBooleanValue(false), "Boolean values shouldn't be equal.")
        XCTAssert(AvroValue.AvroBooleanValue(false) != AvroValue.AvroBooleanValue(true), "Boolean values shouldn't be equal.")
        XCTAssert(AvroValue.AvroBooleanValue(true) != AvroValue.AvroNullValue, "Boolean values shouldn't be equal to values of another type.")
    }

    func testIntEquality() {
        // Generate some non-equal random values
        let i1 = unsafeBitCast(arc4random(), Int32.self)
        var i2 = unsafeBitCast(arc4random(), Int32.self)

        while (i1 == i2) {
            i2 = unsafeBitCast(arc4random(), Int32.self)
        }

        XCTAssert(AvroValue.AvroIntValue(i1) == AvroValue.AvroIntValue(i1), "Integer values should be equal.")
        XCTAssert(AvroValue.AvroIntValue(i2) != AvroValue.AvroIntValue(i1), "Integer values shouldn't be equal.")
        XCTAssert(AvroValue.AvroIntValue(i1) != AvroValue.AvroIntValue(i2), "Integer values shouldn't be equal.")
        XCTAssert(AvroValue.AvroIntValue(i1) != AvroValue.AvroNullValue, "Integer values shouldn't be equal to values of another type.")
    }

    func testLongEquality() {
        // Generate some non-equal random values
        let l1 = Int64(arc4random())
        var l2 = Int64(arc4random())

        while (l1 == l2) {
            l2 = Int64(arc4random())
        }

        XCTAssert(AvroValue.AvroLongValue(l1) == AvroValue.AvroLongValue(l1), "Long values should be equal.")
        XCTAssert(AvroValue.AvroLongValue(l2) != AvroValue.AvroLongValue(l1), "Long values shouldn't be equal.")
        XCTAssert(AvroValue.AvroLongValue(l1) != AvroValue.AvroLongValue(l2), "Long values shouldn't be equal.")
        XCTAssert(AvroValue.AvroLongValue(l1) != AvroValue.AvroNullValue, "Long values shouldn't be equal to values of another type.")
    }

    func testFloatEquality() {
        // Generate some non-equal random values
        let f1 = Float(arc4random())
        var f2 = Float(arc4random())

        while (f1 == f2) {
            f2 = Float(arc4random())
        }

        XCTAssert(AvroValue.AvroFloatValue(f1) == AvroValue.AvroFloatValue(f1), "Float values should be equal.")
        XCTAssert(AvroValue.AvroFloatValue(f2) != AvroValue.AvroFloatValue(f1), "Float values shouldn't be equal.")
        XCTAssert(AvroValue.AvroFloatValue(f1) != AvroValue.AvroFloatValue(f2), "Float values shouldn't be equal.")
        XCTAssert(AvroValue.AvroFloatValue(f1) != AvroValue.AvroNullValue, "Float values shouldn't be equal to values of another type.")
    }

    func testDoubleEquality() {
        // Generate some non-equal random values
        let d1 = Double(arc4random())
        var d2 = Double(arc4random())

        while (d1 == d2) {
            d2 = Double(arc4random())
        }

        XCTAssert(AvroValue.AvroDoubleValue(d1) == AvroValue.AvroDoubleValue(d1), "Double values should be equal.")
        XCTAssert(AvroValue.AvroDoubleValue(d2) != AvroValue.AvroDoubleValue(d1), "Double values shouldn't be equal.")
        XCTAssert(AvroValue.AvroDoubleValue(d1) != AvroValue.AvroDoubleValue(d2), "Double values shouldn't be equal .")
        XCTAssert(AvroValue.AvroDoubleValue(d1) != AvroValue.AvroNullValue, "Double values shouldn't be equal to values of another type.")
    }

    func testStringEquality() {
        let s1 = "Wolf"
        let s2 = "Hound"

        XCTAssert(AvroValue.AvroStringValue(s1) == AvroValue.AvroStringValue(s1), "String values should be equal.")
        XCTAssert(AvroValue.AvroStringValue(s2) != AvroValue.AvroStringValue(s1), "String values shouldn't be equal.")
        XCTAssert(AvroValue.AvroStringValue(s1) != AvroValue.AvroStringValue(s2), "String values shouldn't be equal.")
        XCTAssert(AvroValue.AvroStringValue(s1) != AvroValue.AvroNullValue, "String values shouldn't be equal to values of another type.")
    }

    func testBytesEquality() {

        let b1 = rbyteArray
        let b2 = rbyteArray

        XCTAssert(AvroValue.AvroBytesValue(b1) == AvroValue.AvroBytesValue(b1), "Byte array values should be equal.")
        XCTAssert(AvroValue.AvroBytesValue(b2) != AvroValue.AvroBytesValue(b1), "Byte array values shouldn't be equal.")
        XCTAssert(AvroValue.AvroBytesValue(b1) != AvroValue.AvroBytesValue(b2), "Byte array values shouldn't be equal.")
        XCTAssert(AvroValue.AvroBytesValue(b1) != AvroValue.AvroNullValue, "Byte array values shouldn't be equal to values of another type.")
    }

    func testArrayEquality() {
        let a1 = AvroValue.AvroArrayValue(rbyteArray.map { byte in return AvroValue.AvroIntValue(Int32(byte)) })
        let a2 = AvroValue.AvroArrayValue(rbyteArray.map { byte in return AvroValue.AvroIntValue(Int32(byte)) })

        XCTAssert(a1 == a1, "Array values should be equal.")
        XCTAssert(a1 != a2, "Array values shouldn't be equal.")
        XCTAssert(a1 != AvroValue.AvroNullValue, "Array values shouldn't be equal to values of another type.")
    }

    func testMapEquality() {
        let m1 = AvroValue.AvroMapValue(["wolf" : AvroValue.AvroFloatValue(Float(arc4random()))])
        let m2 = AvroValue.AvroMapValue(["wolf" : AvroValue.AvroFloatValue(Float(arc4random()))])
        let m3 = AvroValue.AvroMapValue(["hound" : AvroValue.AvroFloatValue(Float(arc4random()))])

        XCTAssert(m1 == m1, "Map values should be equal.")
        XCTAssert(m1 != m2, "Map values shouldn't be equal.")
        XCTAssert(m1 != m3, "Map values shouldn't be equal.")
        XCTAssert(m3 != m2, "Map values shouldn't be equal.")
        XCTAssert(m1 != AvroValue.AvroNullValue, "Map values shouldn't be equal to values of another type.")
    }

    func testRecordEquality() {
        let r1 = AvroValue.AvroRecordValue(["wolf" : AvroValue.AvroFloatValue(Float(rbyte))])
        let r2 = AvroValue.AvroRecordValue(["wolf" : AvroValue.AvroFloatValue(Float(rbyte))])
        let r3 = AvroValue.AvroRecordValue(["hound" : AvroValue.AvroFloatValue(Float(rbyte))])

        XCTAssert(r1 == r1, "Record values should be equal.")
        XCTAssert(r1 != r2, "Record values shouldn't be equal.")
        XCTAssert(r1 != r3, "Record values shouldn't be equal.")
        XCTAssert(r3 != r2, "Record values shouldn't be equal.")
        XCTAssert(r1 != AvroValue.AvroNullValue, "Record values shouldn't be equal to values of another type.")
    }

    func testUnionEquality() {
        let u1 = AvroValue.AvroUnionValue(Int(rbyte), Box(AvroValue.AvroDoubleValue(Double(arc4random()))))
        let u2 = AvroValue.AvroUnionValue(Int(rbyte), Box(AvroValue.AvroDoubleValue(Double(arc4random()))))
        let u3 = AvroValue.AvroUnionValue(Int(rbyte), Box(AvroValue.AvroFloatValue(Float(arc4random()))))

        XCTAssert(u1 == u1, "Union values should be equal.")
        XCTAssert(u1 != u2, "Union values shouldn't be equal.")
        XCTAssert(u1 != u3, "Union values shouldn't be equal.")
        XCTAssert(u3 != u2, "Union values shouldn't be equal.")
        XCTAssert(u1 != AvroValue.AvroNullValue, "Union values shouldn't be equal to values of another type.")
    }

    func testEnumEquality() {
        let e1 = AvroValue.AvroEnumValue(Int(rbyte), "Wolf")
        let e2 = AvroValue.AvroEnumValue(Int(rbyte), "Wolf")
        let e3 = AvroValue.AvroEnumValue(Int(rbyte), "Hound")

        XCTAssert(e1 == e1, "Enum values should be equal.")
        XCTAssert(e1 != e2, "Enum values shouldn't be equal.")
        XCTAssert(e1 != e3, "Enum values shouldn't be equal.")
        XCTAssert(e3 != e2, "Enum values shouldn`t be equal.")
        XCTAssert(e1 != AvroValue.AvroNullValue, "Enum values shouldn't be equal to values of another type.")
    }

    func testFixedEquality() {
        let b1 = rbyteArray
        let b2 = rbyteArray

        XCTAssert(AvroValue.AvroFixedValue(b1) == AvroValue.AvroFixedValue(b1), "Fixed values should be equal.")
        XCTAssert(AvroValue.AvroFixedValue(b2) != AvroValue.AvroFixedValue(b1), "Fixed values shouldn't be equal.")
        XCTAssert(AvroValue.AvroFixedValue(b1) != AvroValue.AvroFixedValue(b2), "Fixed values shouldn't be equal.")
        XCTAssert(AvroValue.AvroFixedValue(b1) != AvroValue.AvroNullValue, "Fixed values shouldn't be equal to values of another type.")
    }

    func testInvalidEquality() {
        XCTAssert(AvroValue.AvroInvalidValue == AvroValue.AvroInvalidValue, "Invalid values should be equal.")
        XCTAssert(AvroValue.AvroInvalidValue != AvroValue.AvroNullValue, "Invalid values shouldn't be equal to values of another type.")
    }
}
