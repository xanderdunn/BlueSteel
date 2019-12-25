//
//  AvroFileContainer.swift
//  BlueSteel
//
//  Created by Stefan Paychère.
//  Copyright © 2019 Myotest. All rights reserved.
//

import Foundation

enum AvroError : Error {
    case unsuportedURLType
    case errorCreatingFile
    case errorOpeningFileForReading
    case errorOpeningFileForWritting
    case errorCreatingDirectory
    case errorCreatingJSONSchema
    case errorWritting
    case errorEncodingHeader
    case errorEncodingObject
    case errorNotAnAvroFile
    case errorReadingFileSchema
    case errorReadingBlockHeader
    case errorReadingObject
    case errorReadingFileSync
}

class AvroFileContainer {
    static let magic: [UInt8] = [0x4f, 0x62, 0x6a, 0x01] // Obj\0x01
    static let avroFileContainerSchema = Schema(
        "{\"type\": \"record\", \"name\": \"org.apache.avro.file.Header\"," +
            "\"fields\" : [" +
            "{\"name\": \"magic\", \"type\": {\"type\": \"fixed\", \"name\": \"Magic\", \"size\": 4}}," +
            "{\"name\": \"meta\", \"type\": {\"type\": \"map\", \"values\": \"bytes\"}}, " +
        "{\"name\": \"sync\", \"type\": {\"type\": \"fixed\", \"name\": \"Sync\", \"size\": 16}}, ]}")!
    
    static let metaDataSchemaKey = "avro.schema"
    static let headerMetaDataKey = "meta"
    static let headerMagicKey = "magic"
    static let headerSyncKey = "sync"
}
