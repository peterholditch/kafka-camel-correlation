package com.appdynamics;


import com.appdynamics.apm.appagent.api.ITransactionDemarcator;
import com.appdynamics.instrumentation.sdk.Rule;
import com.appdynamics.instrumentation.sdk.SDKClassMatchType;
import com.appdynamics.instrumentation.sdk.SDKStringMatchType;
import com.appdynamics.instrumentation.sdk.contexts.ISDKUserContext;
import com.appdynamics.instrumentation.sdk.template.AExit;
import com.appdynamics.instrumentation.sdk.toolbox.reflection.IReflector;
import com.appdynamics.instrumentation.sdk.toolbox.reflection.ReflectorException;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class KafkaProducerInstrumentation extends AExit {

    private static final String CLASS_TO_INSTRUMENT = "org.apache.kafka.clients.producer.KafkaProducer";
    private static final String METHOD_TO_INSTRUMENT = "send";
    private IReflector getHeaders = null;
    private IReflector addHeader = null;
    private IReflector topic = null;

    Collection<UUID> li = new ArrayList<UUID>(10);
    final BlockingQueue<UUID> queue = new ArrayBlockingQueue<>(li.size(), false, li);

    private boolean haveCorrelation = false;

    public KafkaProducerInstrumentation() {
        super();
        boolean searchSuperClass = true;
        getHeaders = getNewReflectionBuilder()
                .invokeInstanceMethod("headers", searchSuperClass)
                .build();

        String[] types = new String[]{String.class.getCanonicalName(),"[B"};

        addHeader = getNewReflectionBuilder().invokeInstanceMethod("add", searchSuperClass, types)
                .build();

        topic = getNewReflectionBuilder().invokeInstanceMethod("topic", searchSuperClass)
                .build();

        //Build a list of 10 Unique Ids
        for(int i=0;i<10;i++) {
            queue.add(UUID.randomUUID());
        }
    }

    @Override
    public List<Rule> initializeRules() {
        List<Rule> result = new ArrayList<>();
        Rule.Builder bldr = new Rule.Builder(CLASS_TO_INSTRUMENT);
        bldr = bldr.classMatchType(SDKClassMatchType.MATCHES_CLASS).classStringMatchType(SDKStringMatchType.EQUALS);
        bldr = bldr.methodMatchString(METHOD_TO_INSTRUMENT).methodStringMatchType(SDKStringMatchType.EQUALS);
        result.add(bldr.build());
        return result;
    }

    @Override
    public boolean isCorrelationEnabled() {
        return true;
    }

    @Override
    public boolean isCorrelationEnabledForOnMethodBegin() {
        return true;
    }

    @Override
    public void marshalTransactionContext(String transactionContext, Object invokedObject, String className, String methodName, Object[] paramValues, Throwable thrownException, Object returnValue, ISDKUserContext context) throws ReflectorException {
        if (haveCorrelation)
            return;
        try {
            if (paramValues != null && paramValues.length > 0) {
                Object o = paramValues[0];
                if (o == null){

                }
                else {
                    Object headers = (Iterable) getHeaders.execute(o.getClass().getClassLoader(), o, new Object[]{});
                    addHeader.execute(headers.getClass().getClassLoader(), headers, new Object[]{ITransactionDemarcator.APPDYNAMICS_TRANSACTION_CORRELATION_HEADER,transactionContext.getBytes()});
                }
            } else{

            }
        } catch (ReflectorException e) {
            getLogger().info("ERROR",e);

        }
    }

    @Override
    public Map<String, String> identifyBackend(Object invokedObject, String className, String methodName, Object[] paramValues, Throwable thrownException, Object returnValue, ISDKUserContext context) throws ReflectorException {

        Map<String, String> map = new HashMap<String, String>();
        Object o = paramValues[0];
        String topicStr = topic.execute(o.getClass().getClassLoader(), o);

        //Add one of 10 uuids to the back end, round robin to avoid backend tier fighting
        UUID uuid = queue.poll();
        map.put("Kafka", topicStr+uuid.toString());
        queue.add(uuid);

        return map;
    }

    @Override
    public boolean resolveToNode() {
        return true;
    }

    @Override
    public boolean identifyOnEnd() {
        return false;
    }
}