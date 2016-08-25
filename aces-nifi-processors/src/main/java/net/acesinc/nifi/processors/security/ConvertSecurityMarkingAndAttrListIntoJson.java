/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.acesinc.nifi.processors.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static javax.sql.rowset.spi.SyncFactory.getLogger;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

/**
 *
 * @author jeremytaylor
 */
@EventDriven
@SideEffectFree
@Tags({"parse", "json", "convert", "marking", "security marking", "classification", "attribute"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Converts security markings from file names and a list of flow attributes to JSON")
@WritesAttribute(attribute = "See additional details", description = "This processor may write or remove zero or more attributes as described in additional details")
public class ConvertSecurityMarkingAndAttrListIntoJson extends AbstractProcessor {

    public static final String CONFIG_FILE_NAME = "application.yaml";
    public static final String DEFAULT_CLASSIFICATION_ATTR = "classificationType";
    public static final String DEFAULT_COMPARTMENTS_ATTR = "compartments";
    public static final String DEFAULT_RELEASABILITIES_ATTR = "releasabilities";
    public static final String DEFAULT_DISSEM_CONTROLS_ATTR = "disseminationControls";
    public static final String JSON_ATTRIBUTE_NAME = "JSONAttributes";
    private static final String AT_LIST_SEPARATOR = ",";
    private static final String APPLICATION_JSON = "application/json";

    public static final PropertyDescriptor ATTRIBUTES_LIST = new PropertyDescriptor.Builder()
            .name("Attributes List")
            .description("Comma separated list of attributes to be included in the resulting JSON. If this value "
                    + "is left empty then all existing Attributes will be included. This list of attributes is "
                    + "case sensitive. If an attribute specified in the list is not found it will be be emitted "
                    + "to the resulting JSON with an empty string or NULL value.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONVERTER_CONFIG = new PropertyDescriptor.Builder()
            .name("converter-config")
            .displayName("Converter Config")
            .description("Specifies the Converter settings to use for this flow")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    public static final PropertyDescriptor RAW_SECURITY_MARKING_FROM_FILE_NAME = new PropertyDescriptor.Builder()
            .name("Raw security marking from file name")
            .description("The raw security marking attribute will NOT be included in the resulting String. If this value "
                    + "is left empty then this will be invalid input.")
            .required(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
            .expressionLanguageSupported(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successfully converted raw security marking attribute to multiple attributes").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed to convert raw security marking attribute to multiple attributes").build();

    private final static List<PropertyDescriptor> properties;
    private final static Set<Relationship> relationships;

    static {

        final List<PropertyDescriptor> _properties = new ArrayList<>();
        _properties.add(ATTRIBUTES_LIST);
        _properties.add(CONVERTER_CONFIG);
        _properties.add(RAW_SECURITY_MARKING_FROM_FILE_NAME);
        properties = Collections.unmodifiableList(_properties);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    public List<String> convertSecMarkingToLevelsFlowAttr(String rawSecMarkingReceived, FlowAttrSecurityConfig secConfig) {
        String level = "";
        String potentialLevel = "JUNK";
        final ComponentLog logger = getLogger();
        String dlm = secConfig.getDelim();
        //1) get classification and convert to official names used
        //a) parse for levels and split of prefix part 1 w/ classification part
        //b) build regex based on config
        String regexForClass = getBuiltRegexForClassification(secConfig);
        Matcher m1 = Pattern.compile(regexForClass).matcher(rawSecMarkingReceived);
        if (m1.matches()) {
            logger.debug("Classification part found.");
            potentialLevel = m1.group(1);
            for (String officialLevel : secConfig.getLevelsToConvertTo()) {
                char officialLevelC = officialLevel.charAt(0);
                char potLevelC = potentialLevel.charAt(0);
                if (potLevelC == officialLevelC) {
                    level = officialLevel;
                    break;
                }
            }
        } else {
            logger.debug("Classification part NOT found.");
        }
        List<String> levels = new ArrayList<>();
        levels.add(level);
        //c) now with what was grabbed, let's map back what we need to return based on the first letter grabbed
        return levels;
    }

    public String getBuiltRegexForClassification(FlowAttrSecurityConfig secConfig) {
        final ComponentLog logger = getLogger();
        String dlm = secConfig.getDelim();
        StringBuilder regexPrefixForClass = new StringBuilder();
        regexPrefixForClass.append("(");
        for (String abbrev : secConfig.getAbbreviatedLevelsCanReceive()) {
            regexPrefixForClass.append(abbrev);
            regexPrefixForClass.append("|");
        }
        for (String classPart : secConfig.getLevelsCanReceive()) {
            regexPrefixForClass.append(classPart);
            regexPrefixForClass.append("|");
        }
        int lastIndexOfPipe = regexPrefixForClass.lastIndexOf("|");
        regexPrefixForClass = regexPrefixForClass.replace(lastIndexOfPipe, regexPrefixForClass.length(), "");
        regexPrefixForClass.append(")");
        String regexSuffixForClass = "[A-Za-z" + dlm + "]+";
        regexPrefixForClass.append(regexSuffixForClass);
        String regexForClass = regexPrefixForClass.toString();
        logger.debug("built regex for classification part:=" + regexForClass);
        return regexForClass;
    }

    public List<String> convertSecMarkingToCompartmentsFlowAttr(String rawSecMarkingReceived, FlowAttrSecurityConfig secConfig) {
        String dlm = secConfig.getDelim();
        List<String> compartments = new ArrayList<>();
        //2) parse for compartments
        for (String comp : secConfig.getCompartments()) {
            if (rawSecMarkingReceived.contains(dlm + comp + dlm)) {
                compartments.add(comp);
            }
        }
        return compartments;
    }

    public List<String> convertSecMarkingToReleasabilitiesFlowAttr(String rawSecurityMarkingReceived, FlowAttrSecurityConfig secConfig) {
        String dlm = secConfig.getDelim();
        List<String> releasabilities = new ArrayList<>();
        //3) parse for releasabilities
        for (String rel : secConfig.getReleasabilities()) {
            //these are at the end, so we don't know if a delimiter will be at the end
            if (rawSecurityMarkingReceived.contains(dlm + rel)) {
                releasabilities.add(rel);
            }
        }
        return releasabilities;
    }

    public List<String> convertSecMarkingToDissemControlsFlowAttr(String rawSecurityMarkingReceived, FlowAttrSecurityConfig secConfig) {
        final ComponentLog logger = getLogger();
        List<String> disseminationControls = new ArrayList<>();
        //4) parse for disseminationControls
        for (String dissem : secConfig.getDisseminationControls()) {
            if (rawSecurityMarkingReceived.contains(dissem)) {
                disseminationControls.add(dissem);
            }
        }
        //b) special case for files that don't have dissem marking, but use releasabilities
        if (disseminationControls.isEmpty()) {//if still empty then do this
            for (String rel : secConfig.getReleasabilities()) {
                if (rawSecurityMarkingReceived.contains(rel)) {
                    //using rel dissem marking
                    logger.debug("special dissem control to add when using releabilities, but need corresponding dissemControl:=" + secConfig.getDisseminationControls()[1]);
                    disseminationControls.add(secConfig.getDisseminationControls()[1]);
                    break;
                }
            }
        }

        return disseminationControls;
    }

    /**
     * Builds the Map of attributes that should be included in the JSON that is
     * emitted from this process.
     *
     * @param ff
     * @param rawSecurityMarking
     * @param atrList
     * @param secConfig
     * @return Map of values that are feed to a Jackson ObjectMapper
     * @throws java.io.IOException
     */
    protected Map<String, Object> buildSecurityAttributesMapForFlowFileAndBringInFlowAttrs(
            FlowFile ff,
            String rawSecurityMarking,
            String atrList,
            FlowAttrSecurityConfig secConfig) throws IOException {
        final ComponentLog logger = getLogger();
        Map<String, Object> atsToWrite = new HashMap<>();

        //If list of attributes specified get only those attributes. Otherwise write them all
        if (StringUtils.isNotBlank(atrList)) {
            String[] ats = StringUtils.split(atrList, AT_LIST_SEPARATOR);
            if (ats != null) {
                for (String str : ats) {
                    String cleanStr = str.trim();
                    String val = ff.getAttribute(cleanStr);
                    if (val != null) {
                        atsToWrite.put(cleanStr, val);
                    } else {
                        atsToWrite.put(cleanStr, "");
                    }
                }
            }

        } else {
            atsToWrite.putAll(ff.getAttributes());
        }

        //If list of attributes specified get only those attributes. Otherwise write them all
        if (StringUtils.isNotBlank(rawSecurityMarking)) {
            Classification classification = new Classification();
            String cleanRawMarking = rawSecurityMarking.trim();
            logger.debug("cleanRawMarking:=" + cleanRawMarking);
            List<String> levels = convertSecMarkingToLevelsFlowAttr(cleanRawMarking, secConfig);
            classification.setLevels(levels);
            List<String> convertedComps = convertSecMarkingToCompartmentsFlowAttr(cleanRawMarking, secConfig);
            classification.setCompartments(convertedComps);
            List<String> convertedRels = convertSecMarkingToReleasabilitiesFlowAttr(cleanRawMarking, secConfig);
            classification.setReleasabilities(convertedRels);
            List<String> convertedDissems = convertSecMarkingToDissemControlsFlowAttr(cleanRawMarking, secConfig);
            classification.setDisseminationControls(convertedDissems);
            atsToWrite.put("classification", classification);

        } else {
            //should not get here, but if we do, we need to throw illegal state as we should not be ingesting w/o classification, but instead force configuration changes
            throw new IllegalStateException("This processor is reaching an illegal state.  Please adjust configuration for rawSecurityMarking to not be blank or null.");
        }
        return atsToWrite;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            FlowAttrSecurityConfig secConfig = objectMapper.readValue(context.getProperty(CONVERTER_CONFIG).getValue(), FlowAttrSecurityConfig.class);
            final Map<String, Object> attrsToUpdate = buildSecurityAttributesMapForFlowFileAndBringInFlowAttrs(original,
                    context.getProperty(RAW_SECURITY_MARKING_FROM_FILE_NAME).evaluateAttributeExpressions(original).getValue(),
                    context.getProperty(ATTRIBUTES_LIST).getValue(),
                    secConfig);

            FlowFile conFlowfile = session.write(original, new StreamCallback() {
                @Override
                public void process(InputStream in, OutputStream out) throws IOException {
                    try (OutputStream outputStream = new BufferedOutputStream(out)) {
                        ObjectMapper objMapper = new ObjectMapper();
                        outputStream.write(objMapper.writeValueAsBytes(attrsToUpdate));
                    }
                }
            });
            conFlowfile = session.putAttribute(conFlowfile, CoreAttributes.MIME_TYPE.key(), APPLICATION_JSON);
            session.transfer(conFlowfile, REL_SUCCESS);

        } catch (IOException ioe) {
            getLogger().error(ioe.getMessage());
            session.transfer(original, REL_FAILURE);
        }
    }
}
