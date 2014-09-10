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

    func testPerformanceExample() {
        self.measureBlock() {

        }
    }

}
