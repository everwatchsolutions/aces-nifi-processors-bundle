/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.nifi.processors.mongodb;

import java.util.ArrayList;
import java.util.List;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;

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

    public static final List<PropertyDescriptor> propDescriptors = new ArrayList<>();

    static {
        propDescriptors.addAll(descriptors);

    }

}
