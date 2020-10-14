# BlueSteel

An Avro encoding/decoding library for Swift.

### Never heard of Avro?

Take a gander at the [official documentation for Avro](http://avro.apache.org/docs/current/) before reading further.


### License

BlueSteel is distributed under [the MIT license](https://github.com/gilt/BlueSteel/blob/master/LICENSE).

BlueSteel is provided for your use—free-of-charge—on an as-is basis. We make no guarantees, promises or apologies. *Caveat developer.*


### Adding BlueSteel to your project

Simply add BlueSteel to your `Package.swift`:

```
dependencies: [
    .package(url: "https://github.com/xanderdunn/BlueSteel.git", .branch: "master")
]
```

#### Use

Once successfully integrated, just add the following statement to any Swift file where you want to use BlueSteel:

```swift
import BlueSteel
```

See [the Integration document](https://github.com/gilt/BlueSteel/blob/master/INTEGRATION.md) for additional details on integrating BlueSteel into your project.

### API documentation

For detailed information on using BlueSteel, [API documentation](https://rawgit.com/gilt/BlueSteel/master/Documentation/API/index.html) is available.


## Usage

Since Avro data is not self describing, we're going to need to supply an Avro Schema before we can (de)serialize any data. Schema enums are constructed from a JSON schema description, in either String or NSData form.

```swift
import BlueSteel

let jsonSchema = "{ \"type\" : \"string\" }"
let schema = Schema(jsonSchema)
```

### Deserializing Avro data

Using the Schema above, we can now decode some Avro binary data.

```swift
let rawBytes: [Byte] = [0x6, 0x66, 0x6f, 0x6f]
let avro = AvroValue(schema: schema, withBytes: rawBytes)
```

We can now get the Swift String from the Avro value above using an optional getter.
```swift
if let avroString = avro.string {
    print(avroString) // Prints "foo"
}
```

### Serializing Swift data

We can use the same Schema above to serialize an AvroValue to binary.

```swift
if let serialized = avro.encode(schema) {
    print(serialized) // Prints [6, 102, 111, 111]
}
```

#### But how do we convert our own Swift types to AvroValue?

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

So that just about covers a very quick introduction to BlueSteel. Please note that BlueSteel is still very early in development and may change significantly.
