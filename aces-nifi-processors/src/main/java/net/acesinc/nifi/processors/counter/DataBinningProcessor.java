/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.acesinc.nifi.processors.counter;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.concurrent.TimeUnit;
import net.acesinc.data.binner.Binner;
import net.acesinc.data.binner.DateBinner;
import net.acesinc.data.binner.DateGranularity;
import net.acesinc.data.binner.GeoTileBinner;
import net.acesinc.data.binner.LiteralBinner;
import net.acesinc.data.binner.MergedBinner;
import net.acesinc.data.binner.NumericBinner;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;

/**
 *
 * @author andrewserff
 */
@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"json", "data", "count", "bin", "sort"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttribute(attribute = "mime.type", description = "Always set to application/json")
@CapabilityDescription("Outputs multiple Bin names with a count based on the input data. A new FlowFile is created "
        + "with the bin name and is routed to the 'success' relationship. If binning"
        + "fails, the original FlowFile is routed to the 'failure' relationship.")
public class DataBinningProcessor extends AbstractProcessor {

    protected static final String OUTPUT_MODE_SINGLE = "single";
    protected static final String OUTPUT_MODE_MULTIPLE = "mulitple";

    public static final PropertyDescriptor BINNER_CONFIG = new PropertyDescriptor.Builder()
            .name("binner-config")
            .displayName("Binner Config")
            .description("Specifies the Binners to use for this flow")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor OUTPUT_MODE = new PropertyDescriptor.Builder()
            .name("output-mode")
            .displayName("Output Mode")
            .description("Specifies the Binners to use for this flow")
            .required(true)
            .allowableValues(OUTPUT_MODE_SINGLE, OUTPUT_MODE_MULTIPLE)
            .defaultValue(OUTPUT_MODE_SINGLE)
            .build();

    public static final Relationship REL_BIN = new Relationship.Builder()
            .name("bin")
            .description("The FlowFile with the generated Bin will be routed to this relationship")
            .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile fails processing for any reason (for example, the FlowFile is not valid JSON), it will be routed to this relationship")
            .build();

    private final static List<PropertyDescriptor> properties;
    private final static Set<Relationship> relationships;
    private List<Binner> configuredBinners;
    private ObjectMapper mapper = new ObjectMapper();

    static {

        final List<PropertyDescriptor> _properties = new ArrayList<>();
        _properties.add(BINNER_CONFIG);
        _properties.add(OUTPUT_MODE);
        properties = Collections.unmodifiableList(_properties);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_BIN);
        _relationships.add(REL_ORIGINAL);
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

    @OnScheduled
    public void setup(final ProcessContext context) throws IOException {
        final ProcessorLog log = getLogger();

        configuredBinners = new ArrayList<>();

        if (context.getProperty(BINNER_CONFIG).isSet()) {
            Map<String, Object> config = mapper.readValue(context.getProperty(BINNER_CONFIG).getValue(), Map.class);
            if (config != null && !config.isEmpty()) {
                List<Map<String, Object>> binners = (List<Map<String, Object>>) config.get("binners");

                Map<String, Binner> binnersByName = new HashMap<>();
                
                for (Map<String, Object> binnerConfig : binners) {
                    String type = (String) binnerConfig.get("type");
                    String binName = (String) binnerConfig.get("binName");
                    String dataFieldName = (String) binnerConfig.get("dataFieldName");
                    if (dataFieldName == null) {
                        dataFieldName = binName;
                    }
                    
                    log.info("Configuring " + type + " [ " + binName + ", " + dataFieldName + " ]");
                    Binner b = null;
                    switch (type) {
                        case "DateBinner": {
                            String gran = (String) binnerConfig.get("granularity");
                            DateGranularity dGran = null;
                            try {
                                dGran = DateGranularity.valueOf(gran);
                            } catch (IllegalArgumentException e) {
                                log.warn("Date granularity was invalid. Defaulting to MIN");
                                dGran = DateGranularity.MIN;
                            }
                            b = new DateBinner(binName, dataFieldName, dGran);
                            
                            configuredBinners.add(b);
                            break;
                        }
                        case "LiteralBinner": {
                            b = new LiteralBinner(binName, dataFieldName);
                            configuredBinners.add(b);
                            break;
                        }
                        case "NumericBinner": {
                            Integer maxLevel = (Integer) binnerConfig.get("maxLevel");
                            if (maxLevel == null) {
                                maxLevel = GeoTileBinner.MAX_LEVEL_DEFAULT;
                            }
                            b = new NumericBinner(binName, dataFieldName, maxLevel);
                            configuredBinners.add(b);
                            break;
                        }
                        case "GeoTileBinner": {
                            Integer maxLevel = (Integer) binnerConfig.get("maxLevel");
                            if (maxLevel == null) {
                                maxLevel = GeoTileBinner.MAX_LEVEL_DEFAULT;
                            }
                            String latFieldName = (String) binnerConfig.get("latFieldName");
                            String lonFieldName = (String) binnerConfig.get("lonFieldName");
                            
                            
                            b = null;
                            
                            if (latFieldName != null && !latFieldName.isEmpty() && lonFieldName != null && !lonFieldName.isEmpty()) {
                                b = new GeoTileBinner(binName, dataFieldName, maxLevel, latFieldName, lonFieldName);
                            } else {
                                b = new GeoTileBinner(binName, dataFieldName, maxLevel);
                            }
                            
                            configuredBinners.add(b);
                            break;
                        }
                        case "MergedBinner": {
                            List<String> binnerNames = (List<String>) binnerConfig.get("binners");
                            List<Binner> matchedBinners = new ArrayList<>();
                            for (String name : binnerNames) {
                                Binner binner = binnersByName.get(name);
                                if (binner != null) {
                                    matchedBinners.add(binner);
                                } else {
                                    log.warn("No binner with name " + name + " has been created yet. Make sure MergedBinners come after named Binners");
                                }
                                
                            }
                            b = new MergedBinner(matchedBinners);
                            
                            configuredBinners.add(b);
                            break;
                        }
                        default: {
                            log.warn("Unknown Binner type [ " + type + " ]. Ignoring...");
                        }
                        
                    }
                    
                    if (b != null) {
                        binnersByName.put(b.getBinName(), b);
                    }
                }
            }
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final ProcessorLog log = getLogger();
        final StopWatch watch = new StopWatch(true);

        try {
            // Read the contents of the FlowFile into a byte array
            final byte[] content = new byte[(int) original.getSize()];
            session.read(original, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    StreamUtils.fillBuffer(in, content, true);
                }
            });

            String json = new String(content);

            List<Map<String, Object>> binAndCount = new ArrayList<>();
            StopWatch totalBinners = new StopWatch(true);
            for (Binner b : configuredBinners) {
                StopWatch binnerWatch = new StopWatch(true);
                log.debug("Running Binner [ " + b.getBinName() + " ]");
                List<String> binNames = b.generateBinNames(json);

                for (final String binName : binNames) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("name", binName);
                    map.put("count", 1);
                    binAndCount.add(map);
                }
                binnerWatch.stop();
                log.trace("Binner [ " + b.getBinName() + " ] took " + binnerWatch.getDuration(TimeUnit.MILLISECONDS) + " ms");
            }
            totalBinners.stop();
            log.debug("All binners took " + totalBinners.getDuration(TimeUnit.MILLISECONDS) + " ms");

            String outputMode = context.getProperty(OUTPUT_MODE).getValue();
            switch (outputMode) {
                case OUTPUT_MODE_SINGLE: {
                    final List<FlowFile> bins = new ArrayList<>();
                    for (Map m : binAndCount) {
                        final String jsonDoc = mapper.writeValueAsString(m);

                        FlowFile bin = session.create(original);
                        bin = session.write(bin, new OutputStreamCallback() {
                            @Override
                            public void process(OutputStream out) throws IOException {
                                out.write(jsonDoc.getBytes("UTF-8"));
                            }
                        });
                        bin = session.putAttribute(bin, CoreAttributes.FILENAME.key(), "bin-" + m.get("name") + ".json");
                        bin = session.putAttribute(bin, CoreAttributes.MIME_TYPE.key(), "application/json");
                        bins.add(bin);
                    }

                    session.transfer(bins, REL_BIN);
                    break;
                }
                case OUTPUT_MODE_MULTIPLE: {
                    final String jsonDoc = mapper.writeValueAsString(binAndCount);
                    FlowFile bin = session.create(original);
                    bin = session.write(bin, new OutputStreamCallback() {
                        @Override
                        public void process(OutputStream out) throws IOException {
                            out.write(jsonDoc.getBytes("UTF-8"));
                        }
                    });
                    bin = session.putAttribute(bin, CoreAttributes.FILENAME.key(), "bins-" + original.getAttribute(CoreAttributes.FILENAME.key()) + ".json");
                    bin = session.putAttribute(bin, CoreAttributes.MIME_TYPE.key(), "application/json");
                    session.transfer(bin, REL_BIN);
                    
                    break;
                }
            }
            session.transfer(original, REL_ORIGINAL);

//            bin = session.putAttribute(bin, CoreAttributes.MIME_TYPE.key(), "application/json");
//                    session.transfer(bin, REL_SUCCESS);
//                    session.getProvenanceReporter().modifyContent(bin, "Binned data field " + b.getDataFieldName() + " with " + b.getClass().getName() + " Binner for Bin Name " + b.getBinName(), stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            watch.stop();
            log.debug("Binned {} in {} ms", new Object[]{original, watch.getDuration(TimeUnit.MILLISECONDS)});
        } catch (Exception e) {
            log.error("Failed to bin {} due to {}", new Object[]{original, e}, e);
            session.transfer(original, REL_FAILURE);
            context.yield();
        }
    }

}
