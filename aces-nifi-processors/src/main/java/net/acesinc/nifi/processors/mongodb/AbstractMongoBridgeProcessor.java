/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.acesinc.nifi.processors.mongodb;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.mongodb.MongoDBClientService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.mongodb.AbstractMongoProcessor;

/**
 * This abstract processor exists to help facilitate the continuing existence of
 * our custom Mongo processors. W/o this abstract class, the custom processors
 * would have to be moved into a different package
 * (org.apache.nifi.processors.mongodb) to inherit some fields that have default
 * package level access that need to be accessed in the static initialization
 * phase. These fields were formally public. Due to a mis-design by the NiFi
 * developers, this abstract class is needed to continue to perform static
 * initialization just like NiFi's own processors do as in GetMongo and
 * PutMongo. This class helps be a bridge and gives back Public (!!!) access
 * where it is indeed badly needed.
 *
 * @author jeremytaylor
 */
@EventDriven
@Tags({"mongodb", "insert", "update", "write", "put"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Writes the contents of a FlowFile to MongoDB")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public abstract class AbstractMongoBridgeProcessor extends AbstractMongoProcessor {
    
    protected static final PropertyDescriptor CLIENT_SERVICE = new PropertyDescriptor.Builder()
        .name("mongo-client-service")
        .displayName("Client Service")
        .description("If configured, this property will use the assigned client service for connection pooling.")
        .required(false)
        .identifiesControllerService(MongoDBClientService.class)
        .build();

    protected static final PropertyDescriptor URI = new PropertyDescriptor.Builder()
        .name("Mongo URI")
        .displayName("Mongo URI")
        .description("MongoURI, typically of the form: mongodb://host1[:port1][,host2[:port2],...]")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    protected static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
        .name("Mongo Database Name")
        .displayName("Mongo Database Name")
        .description("The name of the database to use")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    protected static final PropertyDescriptor COLLECTION_NAME = new PropertyDescriptor.Builder()
        .name("Mongo Collection Name")
        .description("The name of the collection to use")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

//    @Override
//    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
//        List<PropertyDescriptor> baseDescriptors = super.getSupportedPropertyDescriptors();
//        List<PropertyDescriptor> customDescriptors = getCustomPropertyDescriptors();
//        
//        List<PropertyDescriptor> combinedDescriptors = new ArrayList<>();
//        combinedDescriptors.addAll(baseDescriptors);
//        combinedDescriptors.addAll(customDescriptors);
//        
//        return combinedDescriptors;
//    }
//    
//    public abstract List<PropertyDescriptor> getCustomPropertyDescriptors();
}
