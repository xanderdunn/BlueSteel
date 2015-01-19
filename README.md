
# Avro Blue Steel

The [nuclear option](http://en.wikipedia.org/wiki/Blue_Steel_(missile)) for working with Avro schemas and binary data, in Swift.

## Never heard of Avro?

Have a gander at the [official docs](http://avro.apache.org/docs/current/) before reading further.

## Requirements

- iOS 8.0+ / Mac OS X 10.9+
- Xcode 6.1

## Integration

BlueSteel should be installed via [Carthage](https://github.com/Carthage/Carthage).
Add the following to your Cartfile:
```
github "gilt/BlueSteel"
```

And then run:
```
$ carthage update
```

BlueSteel will be built as a dynamic framework, which can then be added to your application.

## Usage

BlueSteel depends on [LlamaKit](https://github.com/LlamaKit/LlamaKit) for Result types. You should take a look at the LlamaKit Readme to see how Results work before reading further. They're really simple,  and so it'll just take a minute to get up to speed. Results are very powerful though, and BlueSteel uses them to simplify error reporting.   
Since Avro data is not self describing, we're going to need to supply an Avro Schema before we can (de)serialize any data. Schema enums are constructed from a JSON schema description, in either String or NSData form.

```swift
import LlamaKit
import BlueSteel

let jsonSchema = "{ \"type\" : \"string\" }"
let schema = Schema(string: jsonSchema)
```

## Deserializing Avro data

Using the Schema above, we can now decode some Avro binary data.
First we create a decoder using the schema above and the raw data we want to decode.

```swift
let rawBytes: [Byte] = [0x6, 0x66, 0x6f, 0x6f]
let avroDecoder = AvroDecoder(schema: schema, data: rawBytes)
```

We can then decode an Avro value.

```swift
let avroValueResult = avroDecoder.decodeValue()
```
```decodeValue()``` returns a ```Result<AvroValue, NSError>```, from which we can obtain an Avro value, using the result's ```value``` optional getter. We can then chain the AvroValue ```string``` optional getter.

```swift
if let strValue = avroValueResult.value?.string {
    println(strValue)
}
```

## Serializing Swift data

We can use the same Schema above to create an encdoer that can serialize AvroValues to Avro binary.

```swift
let avroEncoder = AvroEncoder(schema: schema)
```
One or more Avro values can then be emitted to the encoder.

```swift
let avroEncoder = AvroEncoder(schema: schema)
let emitResult = avroEncoder.emitValue(avroValueResult.value!)
```
The encoded stream of bytes can be accessed via the ```data``` or ```byteArray```encoder properties.

```swift
println(avroEncoder.byteArray)  // [6, 102, 111, 111]
```
### What about codec errors?

Comprehensive error reporting is still in progress, but for now if anything goes bad, the results returned by ```decodeValue``` and ```encodeValue``` methods will be Failures and will wrap an NSError object containing a description of what went wrong. The above examples assume that the results returned are successful. In the real world, you should check these results for success, and/or use the ```map``` and ```flatMap``` combinators to chain results.

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

So that just about covers a very quick introduction to BlueSteel. Please note that BlueSteel is still very early in development and may change significantly.

## Copyright & License

Avro BlueSteel Library © Copyright 2014, Gilt Groupe.

Licensed under [the MIT license](LICENSE).

