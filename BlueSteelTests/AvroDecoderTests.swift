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
        Decoder = AvroDecoder([0x4, 0x96, 0xde, 0x87, 0x3, 0x3])
    }

    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
        super.tearDown()
    }

    func testDecodeInt() {
        let x = Decoder?.decodeInt()
        let y = Decoder?.decodeInt()
        XCTAssertEqual(x!, 2, "Decode broken.")
        XCTAssertEqual(y!, 3209099, "Decoder broken.")
    }

    func testDecodeLong() {
        XCTAssert(true, "Pass")
    }
}
