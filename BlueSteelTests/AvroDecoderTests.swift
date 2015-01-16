//
//  AvroDecoderTests.swift
//  BlueSteelTests
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import UIKit
import XCTest
import BlueSteel

class AvroDecoderTests: XCTestCase {
    var Decoder: AvroDecoder? = nil

    override func setUp() {
        super.setUp()
        Decoder = AvroDecoder(schema: Schema.AvroInvalidSchema, data:[0x4, 0x96, 0xde, 0x87, 0x3, 0xcd, 0xcc, 0x4c, 0x40, 0x96, 0xde, 0x87, 0x3])
    }

    override func tearDown() {
        super.tearDown()
    }

    func testDecodeInt() {
        let x = Decoder?.decodeInt32()
        let y = Decoder?.decodeInt32()
    }

    func testDecodeLong() {
        XCTAssert(false, "Not implemented.")
    }

    func testDecodeFloat() {
        XCTAssert(false, "Not implemented.")
    }

    func testDecodeDouble() {
        XCTAssert(false, "Not implemented.")
    }

    func testDecodeString() {
        XCTAssert(false, "Not implemented.")
    }

}
