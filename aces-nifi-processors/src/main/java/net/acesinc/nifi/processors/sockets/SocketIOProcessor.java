/**
 * @author - Joshua Standiford
 */
package net.acesinc.nifi.processors.sockets;

import io.socket.emitter.Emitter;
import okhttp3.OkHttpClient;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;

import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.logging.ComponentLog;
import io.socket.client.Socket;
import io.socket.client.IO;

import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.flowfile.FlowFile;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.util.StopWatch;

import java.util.concurrent.atomic.AtomicReference;
import java.io.ByteArrayOutputStream;
import java.util.concurrent.TimeUnit;
import java.net.URISyntaxException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import java.util.Collections;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.net.URI;

@EventDriven
@SideEffectFree
@Tags({"socket-io", "sockets", "io/socket", "socketio", "IO", "broadcast"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("A SocketIO processor which creates a persistent socket connection.  FlowFile attributes can be used as " +
        "the broadcast event.  FlowFile contents are sent as the body of the message")
public class SocketIOProcessor extends AbstractProcessor{

    private static final String BROADCAST_TYPE_EMIT = "emit";
    private static final String BROADCAST_TYPE_SEND = "send";
    private static final String SCHEME_TYPE_HTTPS = "https";
    private static final String SCHEME_TYPE_HTTP = "http";

    private AtomicReference<Socket> socket;
    private String broadcastType;
    private IO.Options opts;
    private String url;

    //NiFi Processor Properties
    /**
     * The SSLContext to use with HTTPS
     */
    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("Specified the SSL Context Service that can be used to create secure connections")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    /**
     * The URI of the SocketIO server to send to.  The value can be entered either manually or through
     * the NiFi variable registry.
     */
    public static final PropertyDescriptor SOCKET_URL = new PropertyDescriptor.Builder()
            .name("socket-host")
            .displayName("Socket Hostname")
            .description("Socket hostname to broadcast to.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /**
     * The broadcast event to distribute with.
     * Emit allows for custom events to be registered and broadcast to.
     * Send sends messages only to the 'message' event.
     */
    public static final PropertyDescriptor SOCKET_BROADCAST = new PropertyDescriptor.Builder()
            .name("broadcast-type")
            .displayName("Broadcast Type")
            .description("How should socket-io send the message.  Emit will broadcast to the specified event-type.  Send only broadcasts using the `message` event.")
            .required(true)
            .allowableValues(BROADCAST_TYPE_EMIT, BROADCAST_TYPE_SEND)
            .defaultValue(BROADCAST_TYPE_SEND)
            .build();

    /**
     * The socket event to broadcast to.  The event value is only used when the user selects the EMIT broadcast type.
     */
    public static final PropertyDescriptor SOCKET_EVENT = new PropertyDescriptor.Builder()
            .name("socket-event")
            .displayName("Socket Event")
            .description("What event type should the socket broadcast to.  This should only be used if the EMIT broadcast type is selected.")
            .defaultValue("message")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /**
     * The amount of time the socket should wait before forwarding the FlowFile to Failure.
     */
    public static final PropertyDescriptor SOCKET_TIMEOUT = new PropertyDescriptor.Builder()
            .name("socket-timeout")
            .displayName("Connection Timeout")
            .description("Max wait time for socket to connect before failing.")
            .defaultValue("10 secs")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    //NiFi Processor routing relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("The original FlowFile will be routed upon a successful broadcast.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("If the FlowFile fails to connect, it will be routed through this relationship")
            .build();

    private final static List<PropertyDescriptor> properties;
    private final static Set<Relationship> relationships;

    static{
        //Adds property entries to the processor
        final List<PropertyDescriptor> _properties = new ArrayList<>();
        _properties.add(SOCKET_URL);
        _properties.add(SOCKET_BROADCAST);
        _properties.add(SSL_CONTEXT_SERVICE);
        _properties.add(SOCKET_EVENT);
        _properties.add(SOCKET_TIMEOUT);
        properties = Collections.unmodifiableList(_properties);

        //Adds FlowFile relationship to the processor
        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_FAILURE);
        _relationships.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    public Set<Relationship> getRelationships(){
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors(){
        return properties;
    }

    /**
     * Overridden init method that creates new Options for Socket.IO and a new AtomicReference Socket object.
     * @param context - ProcessInitializationContext
     */
    @Override
    public void init(final ProcessorInitializationContext context) {
        socket = new AtomicReference<>();
        opts = new IO.Options();
    }

    /**
     * onScheduled will be called when the NiFi processor is started.  This method should
     * reinitialize the SSLContextService and maintain a persistent connection with
     * the SocketIO server specified in the properties.
     * @param context - The processor configuration.  Contains properties used to initialize processor
     */
    @OnScheduled
    public void setupSockets(final ProcessContext context){
        final ComponentLog logger = getLogger();
        SSLContext sslCon;

        url = context.getProperty(SOCKET_URL).evaluateAttributeExpressions().getValue();
        broadcastType = context.getProperty(SOCKET_BROADCAST).getValue();

        //Check if timeout values are valid, otherwise default to 0
        int connectionTimeout = context.getProperty(SOCKET_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();

        //Try to create the socket is the URI is valid
        try {
            URI uri = new URI(url);

            //If the scheme is HTTPS then create an SSLContext.
            if (uri.getScheme().equalsIgnoreCase(SCHEME_TYPE_HTTPS)) {
                SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
                sslCon = sslContextService.createContext();
                X509TrustManager trustMan = sslContextService.createTrustManager();
                OkHttpClient httpClient = new OkHttpClient().newBuilder().sslSocketFactory(sslCon.getSocketFactory(),
                        trustMan).build();

                opts.secure = true;
                opts.callFactory = httpClient;
                opts.webSocketFactory = httpClient;
                IO.setDefaultOkHttpCallFactory(httpClient);
                IO.setDefaultOkHttpWebSocketFactory(httpClient);

                socket.set(createSocket(uri, opts));

            } else {
                socket.set(createSocket(uri, opts));
            }
            socket.get().connect();
        }
        catch(URISyntaxException e){
            logger.error("Invalid URI, unable to create socket.", e);
        }

        //If the socket is successfully created, then try to connect
        if(socket.get() != null) {
            try {
                StopWatch timer = new StopWatch(true);
                while (!socket.get().connected()) {
                    if (connectionTimeout < timer.getElapsed(TimeUnit.SECONDS)) {
                        logger.warn("Socket failed to connect to " + url + " after " + connectionTimeout + " seconds.");

                        break;
                    }
                    TimeUnit.MILLISECONDS.sleep(100);
                }
                timer.stop();
                if (socket.get().connected()) {
                    logger.debug("Socket successfully connected to " + url + " in " + timer.getDuration(TimeUnit.SECONDS) + " seconds.");
                }
            } catch (InterruptedException e) {
                logger.error("TimeUnit.SECONDS.sleep failed. ", e);
            }
        }
        else{
            logger.error("Failed in creating socket.");
        }

    }

    /**
     * onTrigger gets the socket and obtains the SocketIO event to send / emit to.  The broadcast event can come
     * from the FlowFile attribute or entered manually.  If the socket is unable to connect then the FlowFile is transferred
     * to the Failure relationship.  If the socket is successfully connected then the FlowFile body will be sent / emitted.
     * On a successful transmission the original FlowFile is transferred to the Success relationship.
     * @param context - ProcessContext
     * @param session - ProcessSession
     */
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ComponentLog logger = getLogger();
        Socket _socket = socket.get();

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String event = context.getProperty(SOCKET_EVENT).evaluateAttributeExpressions(flowFile).getValue();
        //If the socket isn't initialized or can't connect.  The FlowFile is passed to the failure relationship
        if(_socket == null || !_socket.connected()){
            logger.warn(url + " failed to connect. ");
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
            return;
        }

        out.reset();
        session.exportTo(flowFile, out);

        String msg = out.toString();
        switch(broadcastType){
            //Send to custom event specified by the processor of flowfile
            case BROADCAST_TYPE_EMIT:
                _socket.emit(event, msg);
                break;
            //Send exclusively to `message` event
            case BROADCAST_TYPE_SEND:
                _socket.send(msg);
                break;
            default:
                logger.error("Invalid broadcastType.  Unable to send message");
                break;
        }
        //On a successful broadcast forward FlowFile to SUCCESS relationship.
        session.getProvenanceReporter().send(flowFile, url);
        session.transfer(flowFile, REL_SUCCESS);
    }

    /**
     * OnStopped method that disconnects and destroys socket connection
     */
    @OnStopped
    public void disconnectSockets(){
        socket.get().disconnect();
        socket.set(null);
    }

    /**
     * This method will take in a URI and Options and create a new socket. The behavior of the socket
     * is determined by this method and will be added to the socket list.
     * @param uri - URI of the endpoint to register
     * @param opts - Options for the socket to use
     * @throws IllegalArgumentException - If the URI parameter is null, an argument is thrown
     */
    private Socket createSocket(URI uri, IO.Options opts) throws IllegalArgumentException{
        if(uri == null){
            throw new IllegalArgumentException("URI cannot be null");
        }
        final ComponentLog logger = getLogger();

        Socket _socket = IO.socket(uri, opts);
        //Socket event for successful connection.  Logs URI and success message
        _socket.on(Socket.EVENT_CONNECT, new Emitter.Listener() {
            @Override
            public void call(Object... objects) {
                logger.debug("Socket " + uri.toString() + " connected successfully...");
            }
            //Socket event for disconnection.
        }).on(Socket.EVENT_DISCONNECT, new Emitter.Listener() {
            @Override
            public void call(Object... objects) {
                logger.debug("Socket " + uri.toString() + " disconnected...");
            }
            //Socket event for reconnection attempts.
        }).on(Socket.EVENT_CONNECT_ERROR, new Emitter.Listener() {
            @Override
            public void call(Object... objects) {
                logger.debug("SocketIO Connect Error");
            }
        })
        ;
        return _socket;
    }
}
