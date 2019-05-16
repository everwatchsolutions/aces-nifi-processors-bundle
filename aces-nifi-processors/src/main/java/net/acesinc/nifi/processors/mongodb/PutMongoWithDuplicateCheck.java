/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.acesinc.nifi.processors.mongodb;

import com.mongodb.MongoWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.processors.mongodb.AbstractMongoBridgeProcessor;
import org.bson.Document;

/**
 * This processor will do a simple Mongo insert, but it will actually handle any
 * error that is thrown when the id already exists. This is useful if you do
 * something special when you try to insert an already existing record, like
 * forward it to another processor like our AddToArrayMongo processor.
 *
 * @author andrewserff
 */
@EventDriven
@Tags({"mongodb", "insert", "update", "write", "put"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Writes the contents of a FlowFile to MongoDB")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class PutMongoWithDuplicateCheck extends AbstractMongoBridgeProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are written to MongoDB are routed to this relationship").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be written to MongoDB are routed to this relationship").build();
    public static final Relationship REL_ALREADY_EXISTS = new Relationship.Builder().name("already-exists")
            .description("If the insert fails because the key already exists, the FlowFile will be routed to this relationship").build();

    protected static final String WRITE_CONCERN_ACKNOWLEDGED = "ACKNOWLEDGED";
    protected static final String WRITE_CONCERN_W1 = "W1";
    protected static final String WRITE_CONCERN_W2 = "W2";
    protected static final String WRITE_CONCERN_W3 = "W3";
    protected static final String WRITE_CONCERN_UNACKNOWLEDGED = "UNACKNOWLEDGED";
    protected static final String WRITE_CONCERN_JOURNALED = "JOURNALED";
    protected static final String WRITE_CONCERN_MAJORITY = "MAJORITY";

    static final PropertyDescriptor WRITE_CONCERN = new PropertyDescriptor.Builder()
            .name("Write Concern")
            .description("The write concern to use")
            .required(true)
            .allowableValues(WRITE_CONCERN_ACKNOWLEDGED, WRITE_CONCERN_W1, WRITE_CONCERN_W2, WRITE_CONCERN_W3, WRITE_CONCERN_UNACKNOWLEDGED, WRITE_CONCERN_JOURNALED, WRITE_CONCERN_MAJORITY)
            .defaultValue(WRITE_CONCERN_ACKNOWLEDGED)
            .build();
    static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
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
        //NOTE: This processor did not originally support SSL and has not been tested w/ use of it. 
        //We are now including the SSL CONTEXT SERVICE and the CLIENT AUTH NAME in this processor for now as that was the easiest thing to do for now.
        //We could choose later to filter out these, but for now they are there.
        _propertyDescriptors.addAll(propDescriptors);
        _propertyDescriptors.add(WRITE_CONCERN);
        _propertyDescriptors.add(CHARACTER_SET);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        _relationships.add(REL_ALREADY_EXISTS);
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
        StopWatch totalWatch = new StopWatch(true);
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
        final WriteConcern writeConcern = getWriteConcern(context);

        final MongoCollection<Document> collection = getCollection(context, flowFile).withWriteConcern(writeConcern);

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
            final Document doc = Document.parse(new String(content, charset));

            StopWatch insertWatch = new StopWatch(true);
            collection.insertOne(doc);
            insertWatch.stop();
            logger.info("inserted {} into MongoDB in {} ms", new Object[]{flowFile, insertWatch.getDuration(TimeUnit.MILLISECONDS)});

            session.getProvenanceReporter().send(flowFile, this.getURI(context));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            if (e instanceof MongoWriteException) {
                if (e.getMessage().contains("duplicate key")) {
                    session.getProvenanceReporter().send(flowFile, this.getURI(context));
                    session.transfer(flowFile, REL_ALREADY_EXISTS);
                    context.yield();
                }
            } else {
                logger.error("Failed to insert {} into MongoDB due to {}", new Object[]{flowFile, e}, e);
                //else, some other error that we don't handle. 
                session.transfer(flowFile, REL_FAILURE);
                context.yield();
            }
        }
        totalWatch.stop();
        logger.info("PutMongo took: " + totalWatch.getDuration(TimeUnit.MILLISECONDS) + "ms");
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

}
