//
//  CCWrapper.c
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

#include <CommonCrypto/CommonCrypto.h>

unsigned char *BlueSteel_SHA256(const void *data, uint32_t len, unsigned char *md) {
    return CC_SHA256(data, len, md);
}
