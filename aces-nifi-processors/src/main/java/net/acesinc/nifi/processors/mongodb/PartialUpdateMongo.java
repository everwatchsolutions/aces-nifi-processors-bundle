/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.acesinc.nifi.processors.mongodb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.UpdateResult;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processors.mongodb.AbstractMongoBridgeProcessor;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;
import org.bson.Document;

/**
 *
 * @author andrewserff
 */
@EventDriven
@Tags({"mongodb", "insert", "update", "write", "put"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Updates a MongoDB document using the contents of a FlowFile")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class PartialUpdateMongo extends AbstractMongoBridgeProcessor {

    protected static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("FlowFiles that resulted in a successful update to some documents in MongoDB are routed to this relationship").build();
    protected static final Relationship REL_SUCCESS_UNMODIFIED = new Relationship.Builder().name("success-unmodified")
            .description("FlowFiles that matched documents but didn't not cause an update to any documents in MongoDB are routed to this relationship").build();
    protected static final Relationship REL_ORIGINAL = new Relationship.Builder().name("original")
            .description("FlowFiles that are processed are routed to this relationship").build();
    protected static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be written to MongoDB are routed to this relationship").build();

    protected static final String MODE_SINGLE = "single";
    protected static final String MODE_MANY = "many";

    protected static final String OPERATION_ADD_TO_SET = "$addToSet";
    protected static final String OPERATION_SET = "$set";
    protected static final String OPERATION_CURRENT_DATE = "$currentDate";
    protected static final String OPERATION_INC = "$inc";
    /**
     * $each is not to be viewable in the list of options. This is used in
     * conjuction with $addToSet.
     */
    protected static final String OPERATION_EACH = "$each";
    private static final String MONGO_TIME_ZONE = "GMT-0";
    private static final String MONGO_DATE_TEMPLATE = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    protected static final String WRITE_CONCERN_ACKNOWLEDGED = "ACKNOWLEDGED";
    protected static final String WRITE_CONCERN_W1 = "W1";
    protected static final String WRITE_CONCERN_W2 = "W2";
    protected static final String WRITE_CONCERN_W3 = "W3";
    protected static final String WRITE_CONCERN_UNACKNOWLEDGED = "UNACKNOWLEDGED";
    protected static final String WRITE_CONCERN_JOURNALED = "JOURNALED";
    protected static final String WRITE_CONCERN_MAJORITY = "MAJORITY";

    protected static final PropertyDescriptor OPERATION = new PropertyDescriptor.Builder()
            .name("Operation")
            .description("The MongoDB Operation we should perform")
            .required(true)
            .allowableValues(OPERATION_ADD_TO_SET, OPERATION_SET, OPERATION_CURRENT_DATE, OPERATION_INC)
            .build();
    protected static final PropertyDescriptor MODE = new PropertyDescriptor.Builder()
            .name("Mode")
            .description("Directs how we should handle the update query. If mode is single, then only a single record that matches the update query would be updated. If mode is many, all records that match would be updated.")
            .required(true)
            .allowableValues(MODE_SINGLE, MODE_MANY)
            .defaultValue(MODE_SINGLE)
            .build();
    protected static final PropertyDescriptor UPDATE_QUERY_KEY = new PropertyDescriptor.Builder()
            .name("Update Query Key")
            .description("Key name used to build the update query criteria; If empty, an empty query matching all documents will be used. You should use an empty query in conjunction with Mode Many.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    protected static final PropertyDescriptor PROPERTY_NAME = new PropertyDescriptor.Builder()
            .name("Name of the Property to update")
            .description("Key name used to both pull from the input data and update in the existing MongoDB Document. Multiple properties can be set by seperating with a comma ','")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    protected static final PropertyDescriptor WRITE_CONCERN = new PropertyDescriptor.Builder()
            .name("Write Concern")
            .description("The write concern to use")
            .required(true)
            .allowableValues(WRITE_CONCERN_ACKNOWLEDGED, WRITE_CONCERN_W1, WRITE_CONCERN_W2, WRITE_CONCERN_W3, WRITE_CONCERN_UNACKNOWLEDGED, WRITE_CONCERN_JOURNALED, WRITE_CONCERN_MAJORITY)
            .defaultValue(WRITE_CONCERN_ACKNOWLEDGED)
            .build();
    protected static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("The Character Set in which the data is encoded")
            .required(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .build();

    private final static Set<Relationship> relationships;
    private final static List<PropertyDescriptor> propertyDescriptors;
    private ObjectMapper mapper = new ObjectMapper();

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        //NOTE: This processor did not originally support SSL and has not been tested w/ use of it. 
        //We are now including the SSL CONTEXT SERVICE and the CLIENT AUTH NAME in this processor for now as that was the easiest thing to do for now.
        //We could choose later to filter out these, but for now they are there.
        _propertyDescriptors.addAll(propDescriptors);
        _propertyDescriptors.add(MODE);
        _propertyDescriptors.add(UPDATE_QUERY_KEY);
        _propertyDescriptors.add(OPERATION);
        _propertyDescriptors.add(PROPERTY_NAME);
        _propertyDescriptors.add(WRITE_CONCERN);
        _propertyDescriptors.add(CHARACTER_SET);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_SUCCESS_UNMODIFIED);
        _relationships.add(REL_ORIGINAL);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());

        try {
            // Read the contents of the FlowFile into a byte array
            final byte[] content = new byte[(int) flowFile.getSize()];
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    StreamUtils.fillBuffer(in, content, true);
                }
            });

            // parse
            // document should look like:
//            {
//              "_id": "myid",
//              "myarray": "newValue"
//            }
            // or to add multiple values (I think this will work...
//            {
//              "_id": "myid",
//              "myarray": { $each: ["newValue1", "newValue2"]}
//            }
            String jsonString = new String(content, charset);
            if (jsonString.isEmpty()) {
                logger.warn("Cannot update with empty document");
                session.transfer(flowFile, REL_FAILURE);
                context.yield();
            } else if (jsonString.startsWith("[")) { //array
                List<Map<String, Object>> docs = mapper.readValue(jsonString, List.class);
                List<Map<String, Document>> updateDocs = new ArrayList<>();
                List<Document> updatedDocs = new ArrayList<>();
                for (Map<String, Object> map : docs) {
                    final Document doc = new Document(map);

                    updateDocs.add(prepareUpdate(flowFile, doc, context, session));
                    updatedDocs.add(doc);
                }

                BulkWriteResult result = performBulkUpdate(flowFile, updateDocs, context, session);

                //clean up after ourselves
                for (Document doc : updatedDocs) {
                    FlowFile updated = session.create(flowFile);
                    final String json = doc.toJson();

                    updated = session.write(updated, new OutputStreamCallback() {
                        @Override
                        public void process(OutputStream out) throws IOException {
                            out.write(json.getBytes("UTF-8"));
                        }
                    });
                    updated = session.putAttribute(updated, CoreAttributes.MIME_TYPE.key(), "application/json");

                    transferFlowFile(updated, result, context, session);
                }

                session.transfer(flowFile, REL_ORIGINAL);
            } else { //document
                final Document doc = Document.parse(jsonString);
                UpdateResult result = performUpdate(flowFile, doc, context, session);
                transferFlowFile(flowFile, result, context, session);
            }

        } catch (Throwable e) {
            logger.error("Failed to insert {} into MongoDB due to {}", new Object[]{flowFile, e}, e);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

    protected void transferFlowFile(FlowFile f, Object result, ProcessContext context, ProcessSession session) {
        final ComponentLog logger = getLogger();
        session.getProvenanceReporter().send(f, this.getURI(context));
        if (result != null) {
            long modifiedCount = 0;
            if (result instanceof UpdateResult) {
                modifiedCount = ((UpdateResult) result).getModifiedCount();
            } else if (result instanceof BulkWriteResult) {
                modifiedCount = ((BulkWriteResult) result).getModifiedCount();
            }
            if (modifiedCount > 0) {
                //we actually did update something, so well call this plain ol success
                session.transfer(f, REL_SUCCESS);
            } else {
                //We successfully ran the update, but nothing changed. 
                session.transfer(f, REL_SUCCESS_UNMODIFIED);
            }
        } else {
            logger.warn("Update Document was empty and ther was nothing to do.");
            session.getProvenanceReporter().send(f, this.getURI(context));
            session.transfer(f, REL_SUCCESS_UNMODIFIED);
        }
    }

    protected Map<String, Document> prepareUpdate(FlowFile flowFile, Document doc, ProcessContext context, ProcessSession session) throws IOException {
        final ComponentLog logger = getLogger();

        Map<String, Document> queryAndUpdateDocs = new HashMap<>();

        final String updateKeys = context.getProperty(UPDATE_QUERY_KEY).getValue();
        final String operation = context.getProperty(OPERATION).getValue();
        Document query = null;
        if (updateKeys == null || updateKeys.isEmpty()) {
            query = new Document();
        } else if (updateKeys.contains(",")) {
            query = new Document();
            String[] keys = updateKeys.split(",");
            for (String key : keys) {
                key = key.trim();
//                logger.info("Adding +1 updateKey [ " + key + " ] with value [ " + doc.get(key) + " ]");
                query.append(key, doc.get(key));
            }
        } else {
//            logger.info("Adding one updateKey [ " + updateKeys + " ] with value [ " + doc.get(updateKeys) + " ]");
            query = new Document(updateKeys, doc.get(updateKeys));
        }
        queryAndUpdateDocs.put("query", query);

        final String propertyName = context.getProperty(PROPERTY_NAME).getValue();
//        logger.info("Starting processing of Operation [ " + operation + " ] for propertyName [ " + propertyName + " ]");
        Document updateDocument = new Document();
        if ("*".equals(propertyName)) {
            updateDocument.append(operation, doc);
        } else if (propertyName.contains(",")) {
            //we have multiple properties to update
            Document operationValues = new Document();
            String[] props = propertyName.split(",");
            for (String p : props) {
                p = p.trim();

                if (OPERATION_CURRENT_DATE.equals(operation)) {
                    operationValues.append(p, true);
                } else if (OPERATION_ADD_TO_SET.equals(operation) && (doc.get(p) != null || p.contains("."))) {
                    if (!p.contains(".")) {
                        String jsonString = mapper.writeValueAsString(doc.get(p));
                        //looking for an array to use $each mode
                        if (jsonString.contains("[{")) {
                            //logic for when there is an array of objects in $addToSet-$each mode
                            logger.debug("$addToSet-$each, array of Objects scenario detected: jsonString [ " + jsonString + " ].");
                            List<Map<Object, Object>> operationValueDocs = mapper.readValue(jsonString, List.class);
                            operationValueDocs = fixTimestampsInside(operationValueDocs);
//                db.inventory.update(
//                                   { _id: 2 },
//                                   { $addToSet: { tags: { $each: [ "camera", "electronics", "accessories" ] } } }
//                                   )

                            Document eachDoc = new Document(OPERATION_EACH, operationValueDocs);
                            operationValues.append(p, eachDoc);
                        } else if (jsonString.contains("[") && !jsonString.contains("{")) {
                            //logic for when there is a regular array w/ items in $addToSet-$each mode
                            List<Map<String, Object>> operationValueDocs = mapper.readValue(jsonString, List.class);
//                db.inventory.update(
//                                   { _id: 2 },
//                                   { $addToSet: { tags: { $each: [ "camera", "electronics", "accessories" ] } } }
//                                   )

                            Document eachDoc = new Document(OPERATION_EACH, operationValueDocs);
                            operationValues.append(p, eachDoc);
                        } else {
                            Object origValue = doc.get(p);
//                    logger.info("Adding update for property [ " + p + " ] with value [ " + origValue + " ]");
                            if (origValue != null) {
                                operationValues.append(p, doc.get(p));
                            } else {
                                //input document didn't have the specified key. skipping. 
                                logger.debug("Input document did not have value for key [ " + p + " ]. Skipping");
                            }
                        }
                    } else {
                        //logic if there is a parent.child scenario for $addToSet each, when there are multiple properties listed w/ comma delmiting
                        operationValues = buildDocumentForParentDotChilAddToSetArrayScenario(p, doc, operationValues, true);
                    }
                } else if (OPERATION_SET.equals(operation) && p.contains(".")) {
                    //logic for when $set is an object -- children -- parent.child situation
                    operationValues = buildDocumentForParentDotChildSetScenario(p, doc, operationValues, true);
                } else {
                    Object origValue = doc.get(p);
//                    logger.info("Adding update for property [ " + p + " ] with value [ " + origValue + " ]");
                    if (origValue != null) {
                        operationValues.append(p, doc.get(p));
                    } else {
                        //input document didn't have the specified key. skipping. 
                        logger.debug("Input document did not have value for key [ " + p + " ]. Skipping");
                    }
                }
            }
            if (!operationValues.isEmpty()) {
                updateDocument.append(operation, operationValues);
            } else {
                logger.debug("No operations added. Skipping...");
            }
        } else {
//            logger.info("Adding update for property [ " + propertyName + " ]");
            Document operationValue = null;

            if (OPERATION_CURRENT_DATE.equals(operation)) {
                //prepare to check if this is an array first
                operationValue = new Document(propertyName, true);
                updateDocument.append(operation, operationValue);
            } else if (OPERATION_ADD_TO_SET.equals(operation) && (doc.get(propertyName) != null || propertyName.contains("."))) {
                if (!propertyName.contains(".")) {
                    String jsonString = mapper.writeValueAsString(doc.get(propertyName));
                    //looking for an array to use $each mode
                    if (jsonString.contains("[{")) {
                        //logic for when there is an array of objects
                        logger.debug("$addToSet-$each, array of Objects scenario detected: jsonString [ " + jsonString + " ].");
                        List<Map<Object, Object>> operationValueDocs = mapper.readValue(jsonString, List.class);
                        operationValueDocs = this.fixTimestampsInside(operationValueDocs);
//                db.inventory.update(
//                                   { _id: 2 },
//                                   { $addToSet: { tags: { $each: [ "camera", "electronics", "accessories" ] } } }
//                                   )

                        Document eachDoc = new Document(OPERATION_EACH, operationValueDocs);
                        Document arrayDoc = new Document(propertyName, eachDoc);
                        updateDocument.append(operation, arrayDoc);
                    } else if (jsonString.contains("[") && !jsonString.contains("{")) {
                        //logic for when there is a regular array w/ items
                        List<Map<String, Object>> operationValueDocs = mapper.readValue(jsonString, List.class);
//                db.inventory.update(
//                                   { _id: 2 },
//                                   { $addToSet: { tags: { $each: [ "camera", "electronics", "accessories" ] } } }
//                                   )

                        Document eachDoc = new Document(OPERATION_EACH, operationValueDocs);
                        Document arrayDoc = new Document(propertyName, eachDoc);
                        updateDocument.append(operation, arrayDoc);
                    } else {
                        operationValue = new Document(propertyName, doc.get(propertyName));
                        updateDocument.append(operation, operationValue);
                    }
                } else {
                    //addToSet when there is one individual propertyName listed and there is 
                    updateDocument = buildDocumentForParentDotChilAddToSetArrayScenario(propertyName, doc, updateDocument, false);
                }
            } else if (OPERATION_SET.equals(operation) && propertyName.contains(".")) {
                //logic on when $set handles an object -- children -- parent.child item specified
                updateDocument = buildDocumentForParentDotChildSetScenario(propertyName, doc, updateDocument, false);
            } else {
                operationValue = new Document(propertyName, doc.get(propertyName));
                updateDocument.append(operation, operationValue);
            }
        }
        queryAndUpdateDocs.put("update", updateDocument);

        return queryAndUpdateDocs;
    }

    protected BulkWriteResult performBulkUpdate(FlowFile f, List<Map<String, Document>> updateDocs, ProcessContext context, ProcessSession session) {
        final ComponentLog logger = getLogger();
        StopWatch watch = new StopWatch(true);

        logger.debug("Performing Bulk Update of [ " + updateDocs.size() + " ] documents");

        final WriteConcern writeConcern = getWriteConcern(context);
        final MongoCollection<Document> collection = getCollection(context, f).withWriteConcern(writeConcern);

        List<WriteModel<Document>> updates = new ArrayList<>();

        for (Map<String, Document> update : updateDocs) {
            UpdateOneModel<Document> upOne = new UpdateOneModel<>(
                    update.get("query"), // find part
                    update.get("update"), // update part
                    new UpdateOptions().upsert(true) // options like upsert
            );
            updates.add(upOne);
        }

        BulkWriteResult bulkWriteResult = collection.bulkWrite(updates, new BulkWriteOptions().ordered(false));
        return bulkWriteResult;

    }

    protected UpdateResult performSingleUpdate(FlowFile f, Document query, Document updateDocument, ProcessContext context, ProcessSession session) {
        final ComponentLog logger = getLogger();
        StopWatch watch = new StopWatch(true);

        final String mode = context.getProperty(MODE).getValue();

        final WriteConcern writeConcern = getWriteConcern(context);
        final MongoCollection<Document> collection = getCollection(context, f).withWriteConcern(writeConcern);

        UpdateResult result = null;
        if (!updateDocument.isEmpty()) {
            watch.start();
//            logger.info("Running Mongo Update with query: " + query + " and document: " + updateDocument);
            switch (mode) {
                case MODE_SINGLE:
                    result = collection.updateOne(query, updateDocument);
                    break;
                case MODE_MANY:
                    result = collection.updateMany(query, updateDocument);
                    break;
            }
            watch.stop();

            logger.info("Running Mongo Update with query: " + query + " and document: " + updateDocument + " took " + watch.getDuration(TimeUnit.MILLISECONDS) + "ms");
            return result;

        } else {
            //nothing to do
            return null;

        }
    }

    protected UpdateResult performUpdate(FlowFile flowFile, Document doc, ProcessContext context, ProcessSession session) throws IOException {
        Map<String, Document> queryAndUpdate = prepareUpdate(flowFile, doc, context, session);

        Document query = queryAndUpdate.get("query");
        Document updateDocument = queryAndUpdate.get("update");

        return performSingleUpdate(flowFile, query, updateDocument, context, session);
    }

    @Override
    protected WriteConcern getWriteConcern(final ProcessContext context) {
        final String writeConcernProperty = context.getProperty(WRITE_CONCERN).getValue();
        WriteConcern writeConcern = null;
        switch (writeConcernProperty) {
            case WRITE_CONCERN_ACKNOWLEDGED:
                writeConcern = WriteConcern.ACKNOWLEDGED;
                break;
            case WRITE_CONCERN_W1:
                writeConcern = WriteConcern.W1;
                break;
            case WRITE_CONCERN_W2:
                writeConcern = WriteConcern.W2;
                break;
            case WRITE_CONCERN_W3:
                writeConcern = WriteConcern.W3;
                break;
            case WRITE_CONCERN_UNACKNOWLEDGED:
                writeConcern = WriteConcern.UNACKNOWLEDGED;
                break;
            case WRITE_CONCERN_JOURNALED:
                writeConcern = WriteConcern.JOURNALED;
                break;
            case WRITE_CONCERN_MAJORITY:
                writeConcern = WriteConcern.MAJORITY;
                break;
            default:
                writeConcern = WriteConcern.ACKNOWLEDGED;
        }
        return writeConcern;
    }

    /**
     * Parses parent.child notation property names that represent a child-key
     * beneath a parent-key in json.
     *
     * @param parentDotChildPropertyName
     * @return
     */
    private String[] parseParentChild(String parentDotChildPropertyName) {
        String[] tmps = parentDotChildPropertyName.split("\\.");
        if (tmps.length != 2) {
            throw new IllegalStateException("IllegalState thrown during use of a parent.child scenario: Use of '.' denotes parent child relationship and is allowed one child dimension in this processor. propertyName [" + parentDotChildPropertyName + "], length [" + tmps.length + "]");
        }
        return tmps;
    }

    /**
     * Used to help build Document for Parent.child $set scenario
     *
     * @param propertyName
     * @param doc
     * @param updateDocument
     * @param isMultiplePropertiesUsed used to designate whether multiple
     * properties are being used w/ comma delimits or not as this does affect
     * logic behavior
     * @return
     * @throws JsonProcessingException
     * @throws IOException
     */
    private Document buildDocumentForParentDotChildSetScenario(String propertyName, Document doc, Document updateDocument, boolean isMultiplePropertiesUsed) throws JsonProcessingException, IOException {
        final ComponentLog logger = getLogger();
        String[] tmps = parseParentChild(propertyName);
        String parentPropertyName = tmps[0];
        String childPropertyName = tmps[1];
        if (doc.get(parentPropertyName) != null && mapper.writeValueAsString(doc.get(parentPropertyName)).contains("{")) {
            String jsonString = mapper.writeValueAsString(doc.get(parentPropertyName));
            if (isMultiplePropertiesUsed) {
                logger.debug("multiple propertyName scenario w/ parent.child notation: $set, p,propertyName:=" + propertyName);
                logger.debug("multiple propertyName scenario w/ parent.child notation: $set, jsonString:=" + jsonString);
            } else {
                logger.debug("single propertyName scenario w/ parent.child notation: $set, propertyName [" + propertyName + "], parentPropertyName [" + parentPropertyName + "], childPropertyName [" + childPropertyName + "]");
                logger.debug("single propertyName scenario w/ parent.child notation: $set, jsonString:=" + jsonString);
            }
            Map<String, Object> operationValueDocs = mapper.readValue(jsonString, Map.class);
            operationValueDocs = this.fixChildOfParentForOpSet(parentPropertyName, childPropertyName, operationValueDocs);
            if (!operationValueDocs.isEmpty()) {
                if (isMultiplePropertiesUsed) {
                    updateDocument.putAll(operationValueDocs);
                } else {
                    updateDocument.append(OPERATION_SET, operationValueDocs);
                }
            } else {
                if (isMultiplePropertiesUsed) {
                    logger.error("Empty document returned when attempting $set w/ parent.child on multiple propertyName scenario. Scenario: $set, propertyName [" + propertyName + "], parentPropertyName [" + parentPropertyName + "], childPropertyName [" + childPropertyName + "]");
                } else {
                    logger.error("Empty document returned when attempting $set w/ parent.child w/ single propertyName scenario. Scenario: $set, propertyName [" + propertyName + "], parentPropertyName [" + parentPropertyName + "], childPropertyName [" + childPropertyName + "]");
                }
            }
        }
        return updateDocument;
    }

    /**
     * Used to help build Document for Parent.child $addToSet-$each scenario
     *
     * @param propertyName
     * @param doc
     * @param updateDocument
     * @param isMultiplePropertiesUsed used to designate whether multiple
     * properties are being used w/ comma delimits or not as this does affect
     * logic behavior
     * @return
     * @throws JsonProcessingException
     * @throws IOException
     */
    private Document buildDocumentForParentDotChilAddToSetArrayScenario(String propertyName, Document doc, Document updateDocument, boolean isMultiplePropertiesUsed) throws JsonProcessingException, IOException {
        final ComponentLog logger = getLogger();
        String[] tmps = parseParentChild(propertyName);
        String parentPropertyName = tmps[0];
        String childPropertyName = tmps[1];
        if (doc.get(parentPropertyName) != null && mapper.writeValueAsString(doc.get(parentPropertyName)).contains("{") && mapper.writeValueAsString(doc.get(parentPropertyName)).contains("[")) {
            //logic on when $addToSet handles an object and has at least one key-child array
            String jsonString = mapper.writeValueAsString(doc.get(parentPropertyName));
            if (isMultiplePropertiesUsed) {
                logger.debug("multiple propertyName scenario w/ parent.child notation: $addToSet, propertyName[" + propertyName + "], parentPropertyName [" + parentPropertyName + "], childPropertyName [" + childPropertyName + "]");
                logger.debug("multiple propertyName scenario w/ parent.child notation: $addToSet, jsonString:=" + jsonString);
            } else {
                logger.debug("single propertyName scenario w/ parent.child notation: $addToSet, propertyName [" + propertyName + "], parentPropertyName [" + parentPropertyName + "], childPropertyName [" + childPropertyName + "]");
                logger.debug("single propertyName scenario w/ parent.child notation: $addToSet, jsonString:=" + jsonString);
            }
            Map<String, Object> operationValueDocs = mapper.readValue(jsonString, Map.class);
            Document parentDoc = this.fixChildOfParentForOpAddToSetEachArray(parentPropertyName, childPropertyName, operationValueDocs);
            if (!parentDoc.isEmpty()) {
                if (isMultiplePropertiesUsed) {
                    updateDocument.putAll(parentDoc);
                } else {
                    updateDocument.append(OPERATION_ADD_TO_SET, parentDoc);
                }
            } else {
                if (isMultiplePropertiesUsed) {
                    logger.error("Empty document returned when attempting $addToSet-$each w/ parent.child w/ multiple propertyNames scenario. Scenario: $addToSet, propertyName [" + propertyName + "], parentPropertyName [" + parentPropertyName + "], childPropertyName [" + childPropertyName + "]");
                } else {
                    logger.error("Empty document returned when attempting $addToSet-$each w/ parent.child w/ single propertyName scenario. Scenario: $addToSet, propertyName [" + propertyName + "], parentPropertyName [" + parentPropertyName + "], childPropertyName [" + childPropertyName + "]");
                }
            }
        }
        return updateDocument;
    }

    /**
     * This is the technique for updating a date field within an object where
     * the object is inside an array. Using the $date technique w/ more nesting
     * like this did not work at all and was rejected.
     *
     * @param opsValueDocs
     * @return
     */
    private List<Map<Object, Object>> fixTimestampsInside(List<Map<Object, Object>> opsValueDocs) {
        final ComponentLog logger = getLogger();
        for (int i = 0; i < opsValueDocs.size(); i++) {
            Map<Object, Object> item = opsValueDocs.get(i);
            Set<Object> keySet = item.keySet();
            Object[] keys = (Object[]) keySet.toArray();
            for (Object key : keys) {
                if (key instanceof String) {
                    logger.debug("$addToSet, adding object scenario that has date child, Key-string: [" + key + "]");
                    String keyStr = (String) key;
                    if (keyStr.toLowerCase().contains("time")) {
                        logger.debug("$addToSet, adding object scenario that has date child, timefield name to update Long w/ ISODate is [" + keyStr + "]");
                        //value must be Long based on obseration, so skip instanceof
                        Long timeVal = (Long) item.get(key);
                        GregorianCalendar gcal = new GregorianCalendar();
                        gcal.setTimeZone(TimeZone.getTimeZone(MONGO_TIME_ZONE));
                        gcal.setTimeInMillis(timeVal);
                        item.put(key, gcal.getTime());
                        opsValueDocs.set(i, item);
                    }
                } else {
                    logger.warn("$addToSet, adding object scenario that has date child, Key-unexpected-obj: [" + key + "]; Class [" + key.getClass().toString() + "]");
                }
            }
        }
        return opsValueDocs;
    }

    /**
     * This helps w/ using $set when there is a parent.child propertyName
     * specified
     *
     * @param parentPropertyName
     * @param childPropertyName
     * @param opsValueDocs
     * @return
     */
    private Map<String, Object> fixChildOfParentForOpSet(String parentPropertyName, String childPropertyName, Map<String, Object> opsValueDocs) {
        Map<String, Object> parentMap = new HashMap<>();
        final ComponentLog logger = getLogger();
        if (opsValueDocs instanceof Map) {
            if (opsValueDocs.containsKey(childPropertyName)) {
                String tmp = parentPropertyName + "." + childPropertyName;
                parentMap.put(tmp, opsValueDocs.get(childPropertyName));
                return parentMap;
            } else {
                logger.error("$set, parent.child scenario: Anticipated child-key was NOT found. child-Key: [" + childPropertyName + "], parentPropertyName [" + parentPropertyName + "]");
            }
        } else {
            logger.error("$set, parent.child scenario: This converted item is not an instance of Map. The class is [" + opsValueDocs.getClass().toString() + "]");
        }
        //otherwise, just return an empty map
        return parentMap;
    }

    /**
     * This helps w/ $addToSet-$each when there is a parent.child propertyName
     * specified and the child item is an array holding child-key
     *
     * @param parentPropertyName
     * @param childPropertyName
     * @param opsValueDocs
     * @return
     */
    private Document fixChildOfParentForOpAddToSetEachArray(String parentPropertyName, String childPropertyName, Map<String, Object> opsValueDocs) {
        final ComponentLog logger = getLogger();
        //db.inventory.update(
        //  { _id: 2 },
        //  { $addToSet: { stuff.tags: { $each: [ "camera", "electronics", "accessories" ] } } }
        //)
        Document emptyDocument = new Document();
        if (opsValueDocs instanceof Map) {
            if (opsValueDocs.containsKey(childPropertyName) && (opsValueDocs.get(childPropertyName) instanceof List)) {
                //for (String key : keys) {
                String tmp = parentPropertyName + "." + childPropertyName;
                //if (opsValueDocs.get(childPropertyName) instanceof List) {
                Document eachDoc = new Document(OPERATION_EACH, opsValueDocs.get(childPropertyName));
                Document arrayDoc = new Document(tmp, eachDoc);
                return arrayDoc;
            } else {
                logger.warn("$addToSet-$each, parent.child scenario for array child: Skipping key-value pair out of map: Key either doesn't exist or Value related to key is not of type List.");
            }

        } else {
            logger.warn("$addToSet-$each, parent.child scenario for array child: This converted item is not an instance of Map. The class is [" + opsValueDocs.getClass().toString() + "]. Investigate further. The Java deserialization from ObjectMapper likely needs to be modified on the data being passed here in the scenario being attempted.");
        }
        //otherwise, just return what we were given
        return emptyDocument;
    }

}
