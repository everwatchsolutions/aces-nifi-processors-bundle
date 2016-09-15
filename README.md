# ACES Custom Apache NiFi Processors

We have found need to create some extensions to Apache NiFi in order to solve some problems the built in processors don't solve for us.  

## Processors

Here is a list of available processors:

### MongoDB

* AddToArrayMongo - Allows you to add elements in an array of a document
* PutMongoWithDuplicationCheck - Extends the PutMongo Processor and adds a new "already-exists" relationship if the document with the same key already exists in mongo

### JSON

* TransformJSON - We ported the TransformJSON processor from NiFi 1.0 to 0.6.1. This is a straight copy of this processor into our bundle. 
* BetterAttributesToJSON - We took some of the original goals that we thought the provided AttributesToJSON should have and added to those by allowing you to more exactly specify whether the attribute in ouputted-JSON format should be a String, Integer, Double, or Date. The current existing AttributesToJSON processor falls well below the needs we have by only outputing things in String form. You simply list the FlowFile expression variables in the corresponding String, Integer, Double, and Date lists and let this processor do the rest.  Dates are expected in Epoch long form down to the millisecond.  A Boolean when listed in the String list will automatically be converted to a Boolean in the output.
* ConvertSecurityMarkingAndAttrListIntoJson - This will take in a raw security marking from a file along with lists of other flow attributes.  The raw security marking will be converted into a Classification JSON object.  The other flow attributes will be included in the JSON conversion.

## Building

Clone this repository and then:

```
git clone git@github.com:acesinc/aces-nifi-processors-bundle
cd aces-nifi-processors-bundle
mvn clean install
```

You will then need to copy the `nar` file into you `nifi/lib` directory

```
cp aces-nifi-nar/target/aces-nifi-nar-0.6.1.nar /path/to/nifi-0.6.1/lib/
```