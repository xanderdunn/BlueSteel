//
//  AvroSchemaTests.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import UIKit
import XCTest
import BlueSteel

class AvroSchemaTests: XCTestCase {

    override func setUp() {
        super.setUp()
    }

    override func tearDown() {
        super.tearDown()
    }

    func testPrimitive() {
        let jsonSchema = "{ \"type\" : \"long\"}"
        var schema = Schema(jsonSchema)

        switch schema {
        case .PrimitiveSchema(.ALong):
            XCTAssert(true, "Passed.")
        default:
            XCTAssert(false, "Failed.")
        }
    }

    func testMap() {
        let jsonSchema = "{ \"type\" : \"map\", \"values\" : \"int\" }"
        var schema = Schema(jsonSchema)

        switch schema {
        case .MapSchema(let box):
            switch box.value {
            case .PrimitiveSchema(.AInt):
                XCTAssert(true, "Passed.")
            default:
                XCTAssert(false, "Failed: Map of wrong type.")
            }
        default:
            XCTAssert(false, "Failed.")
        }
    }

    func testArray() {
        let jsonSchema = "{ \"type\" : \"array\", \"items\" : \"double\" }"
        var schema = Schema(jsonSchema)

        switch schema {
        case .ArraySchema(let box):
            switch box.value {
            case .PrimitiveSchema(.ADouble):
                XCTAssert(true, "Passed.")
            default:
                XCTAssert(false, "Failed: Map of wrong type.")
            }
        default:
            XCTAssert(false, "Failed.")
        }
    }

    func testArrayMap() {
        let jsonSchema = "{ \"type\" : \"array\", \"items\" : { \"type\" : \"map\", \"values\" : \"int\" } }"
        var schema = Schema(jsonSchema)

        switch schema {
        case .ArraySchema(let arrayBox) :
            switch arrayBox.value {
            case .MapSchema(let mapBox) :
                switch mapBox.value {
                case .PrimitiveSchema(.AInt):
                    XCTAssert(true, "Passed.")
                default:
                    XCTAssert(false, "Failed: Map of wrong type.")
                }
            default :
                XCTAssert(false, "Failed: Array of wrong type.")
            }
        default:
            XCTAssert(false, "Failed.")
        }
    }

    func testUnion() {
        let jsonSchema = "{ \"type\" : [ \"double\", \"int\", \"long\", \"float\" ] }"
        let expected: [AvroType] = [.ADouble, .AInt, .ALong, .AFloat]
        var schema = Schema(jsonSchema)

        switch schema {
        case .UnionSchema(let schemas):
            XCTAssert(schemas.count == 4, "Wrong number of schemas in union.")
            for idx in 0...3 {
                switch schemas[idx] {
                case .PrimitiveSchema(expected[idx]) :
                    XCTAssert(true, "Passed")
                default :
                    XCTAssert(false, "Wrong schema type in union.")
                }
            }
        default:
            XCTAssert(false, "Failed.")
        }
    }

    func testUnionMap() {
        let jsonSchema = "{ \"type\" : [ { \"type\" : \"map\", \"values\" : \"int\" }, { \"type\" : \"map\", \"values\" : \"double\" } ] }"
        let expected: [AvroType] = [.AInt, .ADouble]
        var schema = Schema(jsonSchema)

        switch schema {
        case .UnionSchema(let schemas):
            XCTAssert(schemas.count == 2, "Wrong number of schemas in union.")
            for idx in 0...1 {
                switch schemas[idx] {
                case .MapSchema(let box) :
                    switch box.value {
                    case .PrimitiveSchema(expected[idx]) :
                        XCTAssert(true, "Passed")
                    default :
                        XCTAssert(false, "Expected primitive schema type in map.")
                    }
                default :
                    XCTAssert(false, "Expected map schema type in union.")
                }
            }
        default:
            XCTAssert(false, "Failed.")
        }
    }

    func testRecord() {
        let jsonSchema = "{ \"type\" : \"record\", \"name\" : \"AddToCartActionEvent\", " +
            "\"doc\" : \"This event is fired when a user taps on the add to cart button.\"," +
            "\"fields\" : [ { \"name\" : \"lookId\", \"type\" : \"long\" }," +
            "{ \"name\" : \"productId\", \"type\" : \"long\" }," +
            "{ \"name\" : \"quantity\", \"type\" : \"int\" }," +
            "{ \"name\" : \"saleId\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }," +
        "{ \"name\" : \"skuId\",\"type\" : \"long\" }]}"

        let fieldNames = ["lookId", "productId", "quantity", "saleId", "skuId"]
        let fieldType: [AvroType] = [.ALong, .ALong, .AInt, .AInvalidType, .ALong]
        let unionFieldTypes: [AvroType] = [.ANull, .ALong]
        var schema = Schema(jsonSchema)

        switch schema {
        case .RecordSchema("AddToCartActionEvent", let fields) :
            XCTAssert(fields.count == 5, "Record schema should consist of 5 fields.")
            for idx in 0...4 {
                switch fields[idx] {
                case .FieldSchema(fieldNames[idx], let typeSchema) :
                    switch typeSchema.value {
                    case .PrimitiveSchema(fieldType[idx]) :
                        XCTAssert(true, "")
                    case .UnionSchema(let unionSchemas) :
                        XCTAssert(unionSchemas.count == 2, "Union schema should consist of 2 fields.")
                        for uidx in 0...1 {
                            switch unionSchemas[uidx] {
                            case .PrimitiveSchema(unionFieldTypes[uidx]) :
                                XCTAssert(true, "")
                            default :
                                XCTAssert(false, "Wrong type in union")
                            }
                        }
                    default :
                        XCTAssert(false, "Wrong field type.")
                    }
                    XCTAssert(true, "")
                default :
                    XCTAssert(false, "Failed.")
                }
            }
        default:
            XCTAssert(false, "Failed.")
        }
    }

    func testEnum() {
        let jsonSchema = "{ \"type\" : \"enum\", \"name\" : \"ChannelKey\", \"doc\" : \"Enum of valid channel keys.\", \"symbols\" : [ \"CityIphone\", \"CityMobileWeb\", \"GiltAndroid\", \"GiltcityCom\", \"GiltCom\", \"GiltIpad\", \"GiltIpadSafari\", \"GiltIphone\", \"GiltMobileWeb\", \"NoChannel\" ]}"

        let expectedSymbols = ["CityIphone", "CityMobileWeb", "GiltAndroid", "GiltcityCom", "GiltCom", "GiltIpad", "GiltIpadSafari", "GiltIphone", "GiltMobileWeb", "NoChannel"]
        var schema = Schema(jsonSchema)

        switch schema {
        case .EnumSchema(let enumName, let symbols) :
            XCTAssertEqual(enumName, "ChannelKey", "Unexpected enum name.")
            XCTAssertEqual(symbols, expectedSymbols, "Symbols dont match.")
        default :
            XCTAssert(false, "Failed")
        }
    }

    func testFixed() {
        let jsonSchema = "{ \"type\" : \"fixed\", \"name\" : \"Uuid\", \"size\" : 16 }"
        var schema = Schema(jsonSchema)
        switch schema {
        case .FixedSchema(let fixedName, let size) :
            XCTAssertEqual("Uuid", fixedName, "Unexpected fixed name.")
            XCTAssertEqual(16, size, "Unexpected fixed size.")
        default :
            XCTAssert(false, "Failed.")
        }
    }

    func testPerformanceExample() {
        self.measureBlock() {

        }
    }
}
