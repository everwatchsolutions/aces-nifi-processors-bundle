/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.acesinc.nifi.processors.metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import net.acesinc.metrics.model.receiving.MetricStatusUpdate;
import static net.acesinc.nifi.processors.metrics.BetterMetricsSender.REL_SUCCESS;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

/**
 *
 * @author jeremytaylor
 */
@EventDriven
@SideEffectFree
@Tags({"reporting", "metrics"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Publishes metrics from NiFi to Better Metrics Service. ")
@WritesAttribute(attribute = "See additional details", description = "This processor may write or remove zero or more attributes as described in additional details")
@SupportsBatching
public class BetterMetricsSender extends AbstractProcessor {

    private static final int TIMEOUT = 540000; //milliseconds

    public static final PropertyDescriptor SSL_CONTEXT = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The SSL Context Service to use in order to secure the client. If specified, the client will accept only HTTPS requests; "
                    + "otherwise, the server will accept only HTTP requests")
            .required(true)
            .identifiesControllerService(SSLContextService.class)
            .build();

    static final PropertyDescriptor METRICS_SERVICE_URL = new PropertyDescriptor.Builder()
            .name("Metrics Collector URL")
            .description("The URL of the Better Metrics Collector Service")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("https://localhost:8443/aemetrics/rest/metrics/receiving")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    static final PropertyDescriptor APPLICATION_ID = new PropertyDescriptor.Builder()
            .name("Application ID")
            .description("The Application ID to be included in the metrics sent to Better Metrics Service")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("nifi")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor ACTION = new PropertyDescriptor.Builder()
            .name("Action")
            .description("The Action to be included in the metrics sent to Better Metrics Service")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("ingest")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor MONGO_COLLECTION = new PropertyDescriptor.Builder()
            .name("Mongo collection")
            .description("The Mongo collection to be included in the metrics sent to Better Metrics Service")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor DATA_TYPE = new PropertyDescriptor.Builder()
            .name("Data-type")
            .description("The Data-type to be included in the metrics sent to Better Metrics Service")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Hostname")
            .description("The Hostname to be included in the metrics sent to Better Metrics Service")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("${hostname(true)}")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor SITE = new PropertyDescriptor.Builder()
            .name("Site")
            .description("The Site to be included in the metrics sent to Better Metrics Service")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successfully read from flowfile and sent metrics.").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed to read from flowfile and send metrics.").build();

    private final static List<PropertyDescriptor> properties;
    private final static Set<Relationship> relationships;

    static {

        final List<PropertyDescriptor> _properties = new ArrayList<>();
        _properties.add(SSL_CONTEXT);
        _properties.add(METRICS_SERVICE_URL);
        _properties.add(APPLICATION_ID);
        _properties.add(ACTION);
        _properties.add(MONGO_COLLECTION);
        _properties.add(DATA_TYPE);
        _properties.add(HOSTNAME);
        _properties.add(SITE);
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

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();

        final SSLContextService sslService = context.getProperty(SSL_CONTEXT).asControllerService(SSLContextService.class);
        if (sslService == null) {
            logger.error("sslService is NULL!!! abort!!!");
            throw new IllegalStateException("Null SSL context service!!!");
        }

        final String metricsCollectorUrl = context.getProperty(METRICS_SERVICE_URL).evaluateAttributeExpressions().getValue();
        final String applicationId = context.getProperty(APPLICATION_ID).evaluateAttributeExpressions().getValue();
        final String action = context.getProperty(ACTION).evaluateAttributeExpressions().getValue();
        final String collection = context.getProperty(MONGO_COLLECTION).evaluateAttributeExpressions().getValue();
        final String dataType = context.getProperty(DATA_TYPE).evaluateAttributeExpressions().getValue();
        final String hostname = context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue();
        final String site = context.getProperty(SITE).evaluateAttributeExpressions().getValue();

        final FlowFile original = session.get();
        if (original == null) {
            //WARNING: there purpose in this block here is to quiet down the alerting within the NiFi UI -- this will log it, but not bog down the UI
//            logger.info("Incoming flow file is null! Abort!"); --> makes logs too large
            //NOTE: rollback and transfer-success did not work will in this scenario even when creating a brand-new session.  
            //Doing a commit is not perfectly correct, but behaves the best in this attempt to quied down the NiFi UI logging alerts for this unrecoverable scenario.
            session.commit();
            return;
        }

        long flowFileSize = original.getSize();
        MetricStatusUpdate metricStatus = new MetricStatusUpdate();
        metricStatus.setTimestamp(new Date());
        metricStatus.setApplicationId(applicationId);
        metricStatus.setAction(action);
        metricStatus.setCollection(collection);
        metricStatus.setDataType(dataType);
        metricStatus.setHostname(hostname);
        metricStatus.setSite(site);
        metricStatus.setDataSizeIngested(flowFileSize);
        SSLConnectionSocketFactory sf = new SSLConnectionSocketFactory(sslService.createSSLContext(SSLContextService.ClientAuth.WANT), new NoopHostnameVerifier());
        HttpClient httpClient = HttpClientBuilder.create().setSSLSocketFactory(sf).build();
        HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory(httpClient);
        requestFactory.setBufferRequestBody(false);//when we have a lot of calls, we should set to false
        requestFactory.setConnectTimeout(TIMEOUT);
        requestFactory.setReadTimeout(TIMEOUT);
        RestTemplate restTemplate = new RestTemplate(requestFactory);
        getLogger().debug("Sending metrics {} to Custom MetricService", new Object[]{metricStatus.toString()});
        HttpHeaders requestHeaders = new HttpHeaders();
        requestHeaders.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<MetricStatusUpdate> requestEntity = new HttpEntity(metricStatus, requestHeaders);
        //DISCLAIMER: this is communicating to an asynch service endpoint.  
        //Unfortunately, this processor runs out of memory on nifi when using the more correct asynch client. 
        //Using this technique simply works better in practice. The service endpoint doesn't return anything now as a precaution.
        ResponseEntity response = restTemplate.exchange(
                metricsCollectorUrl,
                HttpMethod.POST,
                requestEntity,
                String.class
        );

        //expect a null back and throw it away -- we get nothing back
        if (response.getStatusCode().equals(HttpStatus.OK)) {
            getLogger().info("Successfully sent metrics to Custom MetricService at epoch {} ", new Object[]{System.currentTimeMillis()});
            getLogger().info("Successfully received asynch response from Custom MetricService.");
        } else {
            if (response.hasBody()) {
                getLogger().error("Error sending metrics to Custom MetricService due to {} - {}", new Object[]{response.getStatusCode(), response.getBody()});
            } else {
                getLogger().error("Error sending metrics to Custom MetricService due to status code: {}", new Object[]{response.getStatusCode()});
            }
        }
        session.transfer(original, REL_SUCCESS);
    }

}
