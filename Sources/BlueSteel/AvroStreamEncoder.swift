//
//  AvroStreamEncoder.swift
//  BlueSteel
//
//  Created by Stefan Paychère.
//  Copyright © 2019 Myotest. All rights reserved.
//

import Foundation

open class AvroStreamEncoder: AvroEncoder {
    var checkPointByteCount = 0
    
    convenience init(capacity: Int) {
        self.init()
        bytes.reserveCapacity(capacity)
    }

    func setCheckPoint() {
        checkPointByteCount = bytes.count
    }
    
    func revertToCheckPoint() {
        bytes.removeSubrange(checkPointByteCount..<bytes.count)
    }
}