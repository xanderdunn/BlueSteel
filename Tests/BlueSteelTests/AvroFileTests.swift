//
//  AvroFileTests.swift
//  BlueSteelTests
//
//  Created by Stefan Paychère.
//  Copyright © 2019 Myotest. All rights reserved.
//

import XCTest
@testable import BlueSteel

/// Get a resource relative to the caller's file location
/// Inspired by: https://stackoverflow.com/questions/57693818/copying-resource-files-for-xcode-spm-tests
/// The URL is built and verified: if the resource does not exist it returns nil
///
/// It assumes that your resource files are all in the same directory named "Resources", and the resources are one directory up
/// <Some folder>
///  - Resources
///      - <resource files>
///  - <Some source folder>
///      - <source files>
fileprivate extension URL {
    init?(forResource name: String, type: String, sourceFile: StaticString = #file) {
        let callerURL = URL(fileURLWithPath: "\(sourceFile)", isDirectory: false)
        let callerFolderURL = callerURL.deletingLastPathComponent()
        let resourcesFolderURL = callerFolderURL.deletingLastPathComponent().appendingPathComponent("Resources", isDirectory: true)
        let target = resourcesFolderURL.appendingPathComponent("\(name).\(type)", isDirectory: false)

        var isDirectory: ObjCBool = false
        let exists = FileManager.default.fileExists(atPath: target.path, isDirectory: &isDirectory)
        
        if exists && isDirectory.boolValue == false {
            self = target
        } else {
            return nil
        }
    }
}

class AvroFileTests: XCTestCase {
    // MARK: - Helpers
    private static func getJSONfromFile(filename: String, ext: String = "json") -> Dictionary<String, AnyObject>? {
        guard let jsonURL = URL(forResource: filename, type: ext) else {
            XCTFail("Could not find fixture file \(filename).\(ext)")
            return nil
        }
        do {
            let data = try Data(contentsOf: jsonURL, options: .mappedIfSafe)
            let jsonResult = try JSONSerialization.jsonObject(with: data, options: .mutableLeaves)
            if let jsonResult = jsonResult as? Dictionary<String, AnyObject> {
                return jsonResult
            }
        } catch {
            XCTFail("Could not decode fixture file \(filename).\(ext)")
        }
        return nil
    }
    
    private static func getFileURL(filename: String, ext: String = "avro") -> URL? {
        guard let url = URL(forResource: filename, type: ext) else {
            XCTFail("Could not find fixture file \(filename).\(ext)")
            return nil
        }
        return url
    }

    // MARK: - Fixtures
    struct Fixtures {
        struct Schemas {
            static let gps = AvroFileTests.getJSONfromFile(filename: "gps-schema")!
        }
        struct JSONData {
            static let gps = AvroFileTests.getJSONfromFile(filename: "gps")!
        }
        struct Avro {
            static let gps = AvroFileTests.getFileURL(filename: "gps")!
        }
    }
    
    // MARK: - Validation functions used in tests
    typealias RecordComparator = (_ values: [String:AvroValue], _ reference: [String:AnyObject]) -> ()
    typealias RecordCreator = (_ value: [String:AnyObject]) -> ([String:AvroValue])
    
    private func enumerationEntryIndex(schema: [String:AnyObject], enumKey: String, valueToFind: String, value: [String:AnyObject]) -> (Int?) {
        let fields = schema["fields"] as? [[String:AnyObject]]
        let typeField = fields?.filter { item in
            return item["name"] as? String == enumKey
        }
        guard let enumType = typeField?.first?["type"] as? [String: AnyObject] else { XCTFail(); return nil }
        let enumItems = enumType["symbols"] as? [String]
        guard let typeString = value[enumKey] as? String else { XCTFail(); return nil }
        return enumItems?.firstIndex(of: typeString)
    }
    
    private func equalAny<BaseType: Equatable>(lhs: Any, rhs: Any, baseType: BaseType.Type) -> Bool {
        guard
            let lhsEquatable = lhs as? BaseType,
            let rhsEquatable = rhs as? BaseType
            else { return false }
        return lhsEquatable == rhsEquatable
    }
    
    
    private func checkRead(avroURL: URL, refValues: [String:AnyObject], refSchema: [String:AnyObject], comparator: RecordComparator) {
        let reader = AvroFileReader(schema: nil, url: avroURL)
        
        // Check values
        guard let refValuesData = refValues["data"] as? [[String:AnyObject]] else {
            XCTFail("No reference data available")
            return
        }
        
        var index: Int = 0
        do {
            while let avroSample = try reader.read() {
                guard index < refValuesData.count else {
                    XCTFail("Too much samples in avro file")
                    return
                }
                
                let reference = refValuesData[index]
                switch avroSample {
                case .avroRecordValue(let values):
                    comparator(values, reference)
                default:
                    XCTFail("Not the right avro type")
                }
                index += 1
            }
        } catch {
            XCTFail("Exception while reading back samples")
        }
        XCTAssertEqual(index, refValuesData.count)
        
        // Check schema
        guard let schema = reader.schema else { XCTFail(); return }
        guard let json = schema.json() else { XCTFail(); return }
        guard let data = json.data(using: .utf8) else { XCTFail(); return }
        guard let dict = try? JSONSerialization.jsonObject(with: data, options: .mutableLeaves) as? [String : AnyObject] else { XCTFail(); return }
        
        XCTAssertEqual(refSchema["name"] as! String, dict["name"] as! String)
        XCTAssertEqual(refSchema["type"] as! String, dict["type"] as! String)
        guard let lhs = refSchema["fields"] as? [[String:AnyObject]]  else { XCTFail(); return }
        guard let rhs = dict["fields"] as? [[String:AnyObject]] else { XCTFail(); return }
        XCTAssertTrue(equalAny(lhs: lhs, rhs: rhs, baseType: NSObject.self))
        
    }
    
    lazy var testDocumentsDirectory: URL = {
        let urls = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)
        return urls[urls.count-1]
    }()
    
    
    private func checkWrite(refValues: [String:AnyObject], refSchema: [String:AnyObject], comparator: RecordComparator, creator: RecordCreator) {
        func checkAndDeleteFile(file: URL) {
            if FileManager.default.fileExists(atPath: file.path) {
                do {
                    try FileManager.default.removeItem(at: file)
                } catch {
                    print("Failed to delete file \(file.path): \(error)")
                }
            }
        }
        
        let url = testDocumentsDirectory.appendingPathComponent("testing.avro")
        checkAndDeleteFile(file: url)
        
        guard let values = refValues["data"] as? [[String:AnyObject]] else { XCTFail(); return }
        
        guard let data = try? JSONSerialization.data(withJSONObject: refSchema) else { XCTFail(); return }
        guard let schema = Schema(data) else { XCTFail(); return }
        let avroWriter = AvroFileWriter(schema: schema, url: url)
        
        for value in values {
            let avroValue = creator(value)
            let record = AvroValue.avroRecordValue(avroValue)
            do {
                try avroWriter.append(value: record)
            } catch (let error) {
                XCTFail("Error happending value: \(error)")
            }
        }
        
        XCTAssertEqual(values.count, avroWriter.objectCount)
        try? avroWriter.close()
        
        // verify written file
        checkRead(avroURL: url, refValues: refValues, refSchema: refSchema, comparator: comparator)
    }

    // MARK: - GPS
    func testGPSDecode() {
        checkRead(avroURL: Fixtures.Avro.gps, refValues: Fixtures.JSONData.gps, refSchema: Fixtures.Schemas.gps) {values,reference in
            XCTAssertEqual(values["time"]!.double!, reference["time"] as! Double, accuracy: 0.000001)
            XCTAssertEqual(values["workoutTime"]!.double!, reference["workoutTime"] as! Double, accuracy: 0.000001)
            XCTAssertEqual(values["latitude"]!.double!, reference["latitude"] as! Double, accuracy: 0.000001)
            XCTAssertEqual(values["longitude"]!.double!, reference["longitude"] as! Double, accuracy: 0.000001)
            XCTAssertEqual(values["altitude"]!.double!, reference["altitude"] as! Double, accuracy: 0.000001)
            XCTAssertEqual(values["speed"]!.double!, reference["speed"] as! Double, accuracy: 0.000001)
            XCTAssertEqual(values["horizontalAccuracy"]!.double!, reference["horizontalAccuracy"] as! Double, accuracy: 0.000001)
            XCTAssertEqual(values["verticalAccuracy"]!.double!, reference["verticalAccuracy"] as! Double, accuracy: 0.000001)
            XCTAssertEqual(values["distance"]!.double!, reference["distance"] as! Double, accuracy: 0.000001)
            XCTAssertEqual(values["course"]!.double!, reference["course"] as! Double, accuracy: 0.000001)
            XCTAssertEqual(values["floor"]!.integer!, reference["floor"] as! Int32)
        }
    }
    
    func testGPSEncode() {
        checkWrite(refValues: Fixtures.JSONData.gps, refSchema: Fixtures.Schemas.gps, comparator: {values,reference in
            XCTAssertEqual(values["time"]!.double!, reference["time"] as! Double, accuracy: 0.000001)
            XCTAssertEqual(values["workoutTime"]!.double!, reference["workoutTime"] as! Double, accuracy: 0.000001)
           XCTAssertEqual(values["latitude"]!.double!, reference["latitude"] as! Double, accuracy: 0.000001)
            XCTAssertEqual(values["longitude"]!.double!, reference["longitude"] as! Double, accuracy: 0.000001)
            XCTAssertEqual(values["altitude"]!.double!, reference["altitude"] as! Double, accuracy: 0.000001)
            XCTAssertEqual(values["speed"]!.double!, reference["speed"] as! Double, accuracy: 0.000001)
            XCTAssertEqual(values["horizontalAccuracy"]!.double!, reference["horizontalAccuracy"] as! Double, accuracy: 0.000001)
            XCTAssertEqual(values["verticalAccuracy"]!.double!, reference["verticalAccuracy"] as! Double, accuracy: 0.000001)
            XCTAssertEqual(values["distance"]!.double!, reference["distance"] as! Double, accuracy: 0.000001)
            XCTAssertEqual(values["course"]!.double!, reference["course"] as! Double, accuracy: 0.000001)
            XCTAssertEqual(values["floor"]!.integer!, reference["floor"] as! Int32)
        }, creator: {value in
            let values = [
                "time": AvroValue.avroDoubleValue(value["time"] as! Double),
                "workoutTime": AvroValue.avroDoubleValue(value["workoutTime"] as! Double),
                "latitude": AvroValue.avroDoubleValue(value["latitude"] as! Double),
                "longitude": AvroValue.avroDoubleValue(value["longitude"] as! Double),
                "altitude": AvroValue.avroDoubleValue(value["altitude"] as! Double),
                "speed": AvroValue.avroDoubleValue(value["speed"] as! Double),
                "horizontalAccuracy": AvroValue.avroDoubleValue(value["horizontalAccuracy"] as! Double),
                "verticalAccuracy": AvroValue.avroDoubleValue(value["verticalAccuracy"] as! Double),
                "distance": AvroValue.avroDoubleValue(value["distance"] as! Double),
                "course": AvroValue.avroDoubleValue(value["course"] as! Double),
                "floor": AvroValue.avroIntValue(value["floor"] as! Int32),
            ]
            return values
        })
    }

}
