/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.acesinc.nifi.processors.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

/**
 *
 * @author jeremytaylor
 */
@EventDriven
@SideEffectFree
@Tags({"parse", "convert", "marking", "security marking", "classification", "attribute"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Converts security markings from file names to NiFi flow attributes")
@WritesAttribute(attribute = "See additional details", description = "This processor may write or remove zero or more attributes as described in additional details")
public class ConvertSecurityMarkingIntoFlowAttributes extends AbstractProcessor {

    public static final String CONFIG_FILE_NAME = "application.yaml";
    public static final String DEFAULT_CLASSIFICATION_ATTR = "classificationType";
    public static final String DEFAULT_COMPARTMENTS_ATTR = "compartments";
    public static final String DEFAULT_RELEASABILITIES_ATTR = "releasabilities";
    public static final String DEFAULT_DISSEM_CONTROLS_ATTR = "disseminationControls";

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

    public static final PropertyDescriptor CLASSIFICATION_ATTRIBUTE_TO_OUPUT_TO = new PropertyDescriptor.Builder()
            .name("Classification Attribute To Output To")
            .description("The Classification attribute to be included in the resulting String. If this value "
                    + "is left empty then this will be invalid input.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(DEFAULT_CLASSIFICATION_ATTR)
            .expressionLanguageSupported(false)
            .build();
    public static final PropertyDescriptor COMPARTMENTS_ATTRIBUTE_TO_OUPUT_TO = new PropertyDescriptor.Builder()
            .name("Compartments Attribute To Output To")
            .description("The Compartments attribute to be included in the resulting String. If this value "
                    + "is left empty then this will be invalid input.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(DEFAULT_COMPARTMENTS_ATTR)
            .expressionLanguageSupported(false)
            .build();
    public static final PropertyDescriptor RELEASABILITIES_ATTRIBUTE_TO_OUPUT_TO = new PropertyDescriptor.Builder()
            .name("Releasabilities Attribute To Output To")
            .description("The Releasabilities attribute to be included in the resulting String. If this value "
                    + "is left empty then this will be invalid input.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(DEFAULT_RELEASABILITIES_ATTR)
            .expressionLanguageSupported(false)
            .build();
    public static final PropertyDescriptor DISSEMINATION_CONTROLS_ATTRIBUTE_TO_OUPUT_TO = new PropertyDescriptor.Builder()
            .name("Dissemination Controls Attribute To Output To")
            .description("The Dissemination Controls attribute to be included in the resulting String. If this value "
                    + "is left empty then this will be invalid input.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(DEFAULT_DISSEM_CONTROLS_ATTR)
            .expressionLanguageSupported(false)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successfully converted raw security marking attribute to multiple attributes").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed to convert raw security marking attribute to multiple attributes").build();

    private final static List<PropertyDescriptor> properties;
    private final static Set<Relationship> relationships;

    static {

        final List<PropertyDescriptor> _properties = new ArrayList<>();
        _properties.add(CONVERTER_CONFIG);
        _properties.add(RAW_SECURITY_MARKING_FROM_FILE_NAME);
        _properties.add(CLASSIFICATION_ATTRIBUTE_TO_OUPUT_TO);
        _properties.add(COMPARTMENTS_ATTRIBUTE_TO_OUPUT_TO);
        _properties.add(RELEASABILITIES_ATTRIBUTE_TO_OUPUT_TO);
        _properties.add(DISSEMINATION_CONTROLS_ATTRIBUTE_TO_OUPUT_TO);
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

    public String convertSecMarkingToLevelsFlowAttr(String rawSecMarkingReceived, FlowAttrSecurityConfig secConfig) {
        String level = "";
        String potentialLevel = "JUNK";
        final ComponentLog logger = getLogger();
        String dlm = secConfig.getDelim();
        //1) get classification and convert to official names used
        //a) parse for levels and split of prefix part 1 w/ classification part
        //b) build regex based on config
        String regexForClass = getBuiltRegexForClassification(secConfig);
        System.out.println("raw:=" + rawSecMarkingReceived);
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

        //c) now with what was grabbed, let's map back what we need to return based on the first letter grabbed
        return level;
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

    public String convertSecMarkingToCompartmentsFlowAttr(String rawSecMarkingReceived, FlowAttrSecurityConfig secConfig) {
        String dlm = secConfig.getDelim();
        StringBuilder compartments = new StringBuilder();
        //2) parse for compartments
        for (String comp : secConfig.getCompartments()) {
            if (rawSecMarkingReceived.contains(dlm + comp + dlm)) {
                compartments.append(comp);
                compartments.append(" ");
            }
        }
        return compartments.toString().trim().replaceAll("\\s", ",");
    }

    public String convertSecMarkingToReleasabilitiesFlowAttr(String rawSecurityMarkingReceived, FlowAttrSecurityConfig secConfig) {
        String dlm = secConfig.getDelim();
        StringBuilder releasabilities = new StringBuilder();
        //3) parse for releasabilities
        for (String rel : secConfig.getReleasabilities()) {
            //these are at the end, so we don't know if a delimiter will be at the end
            if (rawSecurityMarkingReceived.contains(dlm + rel)) {
                releasabilities.append(rel);
                releasabilities.append(" ");
            }
        }
        return releasabilities.toString().trim().replaceAll("\\s", ",");
    }

    public String convertSecMarkingToDissemControlsFlowAttr(String rawSecurityMarkingReceived, FlowAttrSecurityConfig secConfig) {
        StringBuilder disseminationControls = new StringBuilder();
        //4) parse for disseminationControls
        for (String dissem : secConfig.getDisseminationControls()) {
            if (rawSecurityMarkingReceived.contains(dissem)) {
                disseminationControls.append(dissem);
            }
        }
        //b) special case for files that don't have dissem marking, but use releasabilities
        if (disseminationControls.length() == 0) {//if still empty then do this
            for (String rel : secConfig.getReleasabilities()) {
                if (rawSecurityMarkingReceived.contains(rel)) {
                    //using rel dissem marking
                    disseminationControls.append(secConfig.getDisseminationControls()[1]);
                    break;
                }
            }
        }

        return disseminationControls.toString().trim().replaceAll("\\s", ",");
    }

    /**
     * Builds the Map of attributes that should be included in the JSON that is
     * emitted from this process.
     *
     * @param ff
     * @param rawSecurityMarking
     * @param nameOfClassificationFlowAttr
     * @param nameOfReleabilitiesFlowAttr
     * @param nameOfDissemControlsFlowAttr
     * @param nameOfCompartmentsFlowAttr
     * @param secConfig
     * @return Map of values that are feed to a Jackson ObjectMapper
     * @throws java.io.IOException
     */
    protected Map<String, String> buildAttributesMapForFlowFile(
            FlowFile ff,
            String rawSecurityMarking,
            String nameOfClassificationFlowAttr,
            String nameOfCompartmentsFlowAttr,
            String nameOfReleabilitiesFlowAttr,
            String nameOfDissemControlsFlowAttr,
            FlowAttrSecurityConfig secConfig) throws IOException {
        final ComponentLog logger = getLogger();
        Map<String, String> atsToWrite = new HashMap<>();

        //If list of attributes specified get only those attributes. Otherwise write them all
        if (StringUtils.isNotBlank(rawSecurityMarking)) {
            String cleanRawMarking = rawSecurityMarking.trim();
            logger.debug("cleanRawMarking:=" + cleanRawMarking);
            String convertedClassMarking = convertSecMarkingToLevelsFlowAttr(cleanRawMarking, secConfig);
            logger.debug("convertedClassMarking:=" + convertedClassMarking);
            atsToWrite.put(nameOfClassificationFlowAttr.trim(), convertedClassMarking);
            String convertedComps = convertSecMarkingToCompartmentsFlowAttr(cleanRawMarking, secConfig);
            logger.debug("convertedComps:=" + convertedComps);
            atsToWrite.put(nameOfCompartmentsFlowAttr.trim(), convertedComps);
            String convertedRels = convertSecMarkingToReleasabilitiesFlowAttr(cleanRawMarking, secConfig);
            logger.debug("convertedRels:=" + convertedRels);
            atsToWrite.put(nameOfReleabilitiesFlowAttr.trim(), convertedRels);
            String convertedDissems = convertSecMarkingToDissemControlsFlowAttr(cleanRawMarking, secConfig);
            logger.debug("convertedDissems:=" + convertedDissems);
            atsToWrite.put(nameOfDissemControlsFlowAttr.trim(), convertedDissems);

        } else {
            atsToWrite.put(nameOfClassificationFlowAttr, "");
            atsToWrite.put(nameOfCompartmentsFlowAttr, "");
            atsToWrite.put(nameOfReleabilitiesFlowAttr, "");
            atsToWrite.put(nameOfDissemControlsFlowAttr, "");
        }
        atsToWrite.putAll(ff.getAttributes());
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
            final Map<String, String> flowAttrsToUpdate = buildAttributesMapForFlowFile(original,
                    context.getProperty(RAW_SECURITY_MARKING_FROM_FILE_NAME).evaluateAttributeExpressions(original).getValue(),
                    context.getProperty(CLASSIFICATION_ATTRIBUTE_TO_OUPUT_TO).getValue(),
                    context.getProperty(COMPARTMENTS_ATTRIBUTE_TO_OUPUT_TO).getValue(),
                    context.getProperty(RELEASABILITIES_ATTRIBUTE_TO_OUPUT_TO).getValue(),
                    context.getProperty(DISSEMINATION_CONTROLS_ATTRIBUTE_TO_OUPUT_TO).getValue(),
                    secConfig);

            final FlowFile updated = session.putAllAttributes(original, flowAttrsToUpdate);

            session.transfer(updated, REL_SUCCESS);

        } catch (IOException ioe) {
            getLogger().error(ioe.getMessage());
            session.transfer(original, REL_FAILURE);
        }
    }
}
