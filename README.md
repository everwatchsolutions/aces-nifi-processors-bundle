# ACES Custom Apache NiFi Processors

We have found need to create some extensions to Apache NiFi in order to solve some problems the built in processors don't solve for us.  

## Processors

Here is a list of available processors:

### MongoDB

* AddToArrayMongo - Allows you to add elements in an array of a document
* PutMongoWithDuplicationCheck - Extends the PutMongo Processor and adds a new "already-exists" relationship if the document with the same key already exists in mongo

### JSON

* TransformJSON - We ported the TransformJSON processor from NiFi 1.0 to 0.6.1. This is a straight copy of this processor into our bundle. 

## Building

Clone this repository and then:

```
mvn clean install
```

You will then need to copy the `nar` file into you `nifi/lib` directory

```
cp aces-nifi-nar/target/aces-nifi-nar-0.6.1.nar /path/to/nifi-0.6.1/lib/
```