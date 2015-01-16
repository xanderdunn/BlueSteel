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
