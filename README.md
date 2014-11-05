
# Avro Blue Steel

The [nuclear option](http://en.wikipedia.org/wiki/Blue_Steel_(missile)) for working with Avro schemas and binary data, in Swift.

## Never heard of Avro?

Have a gander at the [official docs](http://avro.apache.org/docs/current/) before reading further.

## Requirements

- iOS 7.0+ / Mac OS X 10.9+
- Xcode 6.1

## Integration

Since there's currently no [proper infrastructure](http://cocoapods.org) for Swift dependency management, using BlueSteel in your project requires a little bit of extra work, as outlined by the following steps:

1. Add BlueSteel as a submodule by opening the Terminal, `cd`-ing into your top-level project directory, and entering the command `git submodule add https://github.com/BlueSteel/BlueSteel.git` and then `git submodule update --init --recursive` to pull in BlueSteel's dependencies.
2. Open the `BlueSteel` folder, and drag `BlueSteel.xcodeproj` into the file navigator of your app project.
3. In Xcode, navigate to the target configuration window by clicking on the blue project icon, and selecting the application target under the "Targets" heading in the sidebar.
4. Ensure that the deployment target of BlueSteel.framework matches that of the application target.
5. In the tab bar at the top of that window, open the "Build Phases" panel.
6. Expand the "Target Dependencies" group, and add `BlueSteel.framework`.
7. Click on the `+` button at the top left of the panel and select "New Copy Files Phase". Rename this new phase to "Copy Frameworks", set the "Destination" to "Frameworks", and add `BlueSteel.framework`.

## Usage

Since Avro data is not self describing, we're going to need to supply an Avro Schema before we can (de)serialize any data. Schema enums are constructed from a JSON schema description, in either String or NSData form.

```swift
import BlueSteel

let jsonSchema = "{ \"type\" : \"string\" }"
let schema = Schema(jsonSchema)
```

## Deserializing Avro data

Using the Schema above, we can now decode some Avro binary data.

```swift
let rawBytes: [Byte] = [0x6, 0x66, 0x6f, 0x6f]
let avro = AvroValue(schema: schema, withBytes: rawBytes)
```

We can now get the Swift String from the Avro value above using an optional getter.
```swift
if let avroString = avro.string {
    println(avroString) // Prints "foo"
}
```

## Serializing Swift data

We can use the same Schema above to serialize an AvroValue to binary.

```swift
if let serialized = avro.encode(schema) {
    println(serialized) // Prints [6, 102, 111, 111]
}
```

### But how do we convert our own Swift types to AvroValue?

By conforming to the AvroValueConvertible protocol! You just need to extend your types with one function:
```swift
func toAvro() -> AvroValue
```

Suppose we wanted to serialize a NSUUID with the following schema:

```JSON
{
    "type" : "fixed",
    "name" : "UUID",
    "size" : 16
}
```

We could extend NSUUID as follows:

```swift
extension NSUUID : AvroValueConvertible {
    public func toAvro() -> AvroValue {
        var uuidBytes: [Byte] = [Byte](count: 16, repeatedValue: 0)
        self.getUUIDBytes(&uuidBytes)
        return AvroValue.AvroFixedValue(uuidBytes)
    }
}
```

To generate and serialize a NSUUID, we could now do:

```swift
let serialized: [Byte]? = NSUUID().toAvro().encode(uuidSchema)
```
Hey presto! We now have a byte array representing an NSUUID serialized to Avro according to the fixed schema provided.
Okay, so the example above is maybe a little bit too simple. Let's take a look at a more complex example. Suppose we have a record schema as follows:

```JSON
{
    "type": "record", 
        "name": "test",
        "fields" : [
        {"name": "a", "type": "long"},
        {"name": "b", "type": "string"}
    ]
}
```

We could create a corresponding type Swift that might look something like this:
```swift
struct testStruct {
    var a: Int64 = 0
    var b: String = ""
}
```

To convert testStruct to AvroValue, we could extend it like this:

```swift
extension testStruct : AvroValueConvertible {
    func toAvro() -> AvroValue {
        return AvroValue.AvroRecordValue([
                "a" : self.a.toAvro(),
                "b" : self.b.toAvro()])
    }
}
```

You might've noticed above that we called .toAvro() on Int64 and String values. We didn't have to define these ourselves because BlueSteel provides AvroValueConvertible extensions for Swift primitives.

So that just about covers very quick introduction to BlueSteel. Please note that BlueSteel is still very early in development and may change significantly.

## Copyright & License

Avro BlueSteel Library © Copyright 2014, Gilt Groupe.

Licensed under [the MIT license](LICENSE).

