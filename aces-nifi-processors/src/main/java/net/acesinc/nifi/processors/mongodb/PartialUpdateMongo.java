/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.acesinc.nifi.processors.mongodb;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.UpdateResult;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processors.mongodb.AbstractMongoProcessor;
import org.apache.nifi.stream.io.StreamUtils;
import org.bson.Document;

/**
 *
 * @author andrewserff
 */
@EventDriven
@Tags({"mongodb", "insert", "update", "write", "put"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Updates a MongoDB document using the contents of a FlowFile")
public class PartialUpdateMongo extends AbstractMongoProcessor {

    protected static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("FlowFiles that resulted in a successful update to some documents in MongoDB are routed to this relationship").build();
    protected static final Relationship REL_SUCCESS_UNMODIFIED = new Relationship.Builder().name("success-unmodified")
            .description("FlowFiles that matched documents but didn't not cause an update to any documents in MongoDB are routed to this relationship").build();
    protected static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be written to MongoDB are routed to this relationship").build();

    protected static final String MODE_SINGLE = "single";
    protected static final String MODE_MANY = "many";

    protected static final String OPERATION_ADD_TO_SET = "$addToSet";
    protected static final String OPERATION_SET = "$set";
    protected static final String OPERATION_CURRENT_DATE = "$currentDate";

    protected static final String WRITE_CONCERN_ACKNOWLEDGED = "ACKNOWLEDGED";
    protected static final String WRITE_CONCERN_UNACKNOWLEDGED = "UNACKNOWLEDGED";
    protected static final String WRITE_CONCERN_FSYNCED = "FSYNCED";
    protected static final String WRITE_CONCERN_JOURNALED = "JOURNALED";
    protected static final String WRITE_CONCERN_REPLICA_ACKNOWLEDGED = "REPLICA_ACKNOWLEDGED";
    protected static final String WRITE_CONCERN_MAJORITY = "MAJORITY";

    protected static final PropertyDescriptor OPERATION = new PropertyDescriptor.Builder()
            .name("Operation")
            .description("The MongoDB Operation we should perform")
            .required(true)
            .allowableValues(OPERATION_ADD_TO_SET, OPERATION_SET, OPERATION_CURRENT_DATE)
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
            .description("Key name used to build the update query criteria; this property must be present in the FlowFile content")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("_id")
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
            .allowableValues(WRITE_CONCERN_ACKNOWLEDGED, WRITE_CONCERN_UNACKNOWLEDGED, WRITE_CONCERN_FSYNCED, WRITE_CONCERN_JOURNALED,
                    WRITE_CONCERN_REPLICA_ACKNOWLEDGED, WRITE_CONCERN_MAJORITY)
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

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(URI);
        _propertyDescriptors.add(DATABASE_NAME);
        _propertyDescriptors.add(COLLECTION_NAME);
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

        final ProcessorLog logger = getLogger();

        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
        final WriteConcern writeConcern = getWriteConcern(context);

        final String mode = context.getProperty(MODE).getValue();
        final MongoCollection<Document> collection = getCollection(context).withWriteConcern(writeConcern);

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
            final Document doc = Document.parse(new String(content, charset));

            // now set up the update
            final String updateKeys = context.getProperty(UPDATE_QUERY_KEY).getValue();
            final String operation = context.getProperty(OPERATION).getValue();
            Document query = null;
            if (updateKeys.contains(",")) {
                query = new Document();
                String [] keys = updateKeys.split(",");
                for(String key : keys){
                    key = key.trim();
                    logger.info("Adding +1 updateKey [ "+key+ " ] with value [ "+doc.get(key)+" ]");
                    query.append(key, doc.get(key));
                }
            } else {
                logger.info("Adding one updateKey [ "+updateKeys+ " ] with value [ "+doc.get(updateKeys)+" ]");
                query = new Document(updateKeys, doc.get(updateKeys));
            }

            final String propertyName = context.getProperty(PROPERTY_NAME).getValue();
            logger.info("Starting processing of Operation [ " + operation + " ] for propertyName [ " + propertyName + " ]");
            Document updateDocument = new Document();
            if (propertyName.contains(",")) {
                //we have multiple properties to update
                Document operationValues = new Document();
                String[] props = propertyName.split(",");
                for (String p : props) {
                    p = p.trim();

                    if (!OPERATION_CURRENT_DATE.equals(operation)) {
                        Object origValue = doc.get(p);
                        logger.info("Adding update for property [ " + p + " ] with value [ " + origValue + " ]");
                        if (origValue != null) {
                            operationValues.append(p, doc.get(p));
                        } else {
                            //input document didn't have the specified key. skipping. 
                        }
                    } else {
                        operationValues.append(p, true);
                    }
                }
                if (!operationValues.isEmpty()) {
                    updateDocument.append(operation, operationValues);
                } else {
                    logger.info("No operations added. Skipping...");
                }
            } else {
                logger.info("Adding update for property [ " + propertyName + " ]");
                Document operationValue = null;
                if (!OPERATION_CURRENT_DATE.equals(operation)) {
                    operationValue = new Document(propertyName, doc.get(propertyName));
                } else {
                    operationValue = new Document(propertyName, true);
                }
                updateDocument.append(operation, operationValue);
            }

            UpdateResult result = null;
            if (!updateDocument.isEmpty()) {
                switch (mode) {
                    case MODE_SINGLE:
                        result = collection.updateOne(query, updateDocument);
                        break;
                    case MODE_MANY:
                        result = collection.updateMany(query, updateDocument);
                        break;
                }

                logger.info("updated {} into MongoDB", new Object[]{flowFile});

                session.getProvenanceReporter().send(flowFile, context.getProperty(URI).getValue());
                if (result != null) {
                    //XXX Do we need to be checking isModifiedCountAvailable() ?
                    if (result.getModifiedCount() > 0) {
                        //we actually did update something, so well call this plain ol success
                        session.transfer(flowFile, REL_SUCCESS);
                    } else {
                        //We successfully ran the update, but nothing changed. 
                        session.transfer(flowFile, REL_SUCCESS_UNMODIFIED);
                    }
                }
            } else {
                //nothing to do
                session.getProvenanceReporter().send(flowFile, context.getProperty(URI).getValue());
                session.transfer(flowFile, REL_SUCCESS_UNMODIFIED);
            }
        } catch (Exception e) {
            logger.error("Failed to insert {} into MongoDB due to {}", new Object[]{flowFile, e}, e);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

    protected WriteConcern getWriteConcern(final ProcessContext context) {
        final String writeConcernProperty = context.getProperty(WRITE_CONCERN).getValue();
        WriteConcern writeConcern = null;
        switch (writeConcernProperty) {
            case WRITE_CONCERN_ACKNOWLEDGED:
                writeConcern = WriteConcern.ACKNOWLEDGED;
                break;
            case WRITE_CONCERN_UNACKNOWLEDGED:
                writeConcern = WriteConcern.UNACKNOWLEDGED;
                break;
            case WRITE_CONCERN_FSYNCED:
                writeConcern = WriteConcern.FSYNCED;
                break;
            case WRITE_CONCERN_JOURNALED:
                writeConcern = WriteConcern.JOURNALED;
                break;
            case WRITE_CONCERN_REPLICA_ACKNOWLEDGED:
                writeConcern = WriteConcern.REPLICA_ACKNOWLEDGED;
                break;
            case WRITE_CONCERN_MAJORITY:
                writeConcern = WriteConcern.MAJORITY;
                break;
            default:
                writeConcern = WriteConcern.ACKNOWLEDGED;
        }
        return writeConcern;
    }

}
