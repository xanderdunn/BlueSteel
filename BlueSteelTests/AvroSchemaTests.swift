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

    func testPerformanceExample() {
        self.measureBlock() {

        }
    }

}
