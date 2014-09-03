//
//  VarintTests.swift
//  BlueSteelTests
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import UIKit
import XCTest
import BlueSteel

class VarintTests: XCTestCase {
    
    override func setUp() {
        super.setUp()
    }
    
    override func tearDown() {
        super.tearDown()
    }

    func testToInt() {
        //TODO: Exercise a range of numbers here.
        let expected = Int(bitPattern: UInt(arc4random()))
        let testvarint = Varint(fromValue: expected)
        let val = testvarint.toInt()
        println(testvarint.description)
        XCTAssertEqual(val, expected, "Expected -1. Got\(val)")
    }

    func testToUInt() {
        let expected = UInt(arc4random())
        let testvarint = Varint(fromValue: expected)
        let val = testvarint.toUInt()
        println(testvarint.description)
        XCTAssertEqual(val, expected, "Expected -1. Got\(val)")
    }

    func testEncodeZigZag() {
        let val = Int(Int32.max).encodeZigZag()
        XCTAssertEqual(val, UInt(UInt32.max) - 1, "\(val)")
    }

    func testDecodeZigZag() {
        let val = UInt.max.decodeZigZag()
        XCTAssertEqual(val, Int.min, "\(val)")
    }
}
