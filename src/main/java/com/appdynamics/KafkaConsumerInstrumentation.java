package com.appdynamics;

import com.appdynamics.apm.appagent.api.ITransactionDemarcator;
import com.appdynamics.instrumentation.sdk.Rule;
import com.appdynamics.instrumentation.sdk.SDKClassMatchType;
import com.appdynamics.instrumentation.sdk.SDKStringMatchType;
import com.appdynamics.instrumentation.sdk.contexts.ISDKUserContext;
import com.appdynamics.instrumentation.sdk.template.AEntry;
import com.appdynamics.instrumentation.sdk.toolbox.reflection.IReflector;
import com.appdynamics.instrumentation.sdk.toolbox.reflection.ReflectorException;

import java.util.ArrayList;
import java.util.List;

public class KafkaConsumerInstrumentation extends AEntry {

    private static final String CLASS_TO_INSTRUMENT = "org.apache.kafka.clients.consumer.internals.Fetcher";
    private static final String METHOD_TO_INSTRUMENT = "parseRecord";
    private IReflector key = null;
    private IReflector value = null;

    private IReflector getHeaders = null;
    private boolean identifyBt = true;

    public KafkaConsumerInstrumentation() {
        super();
        boolean searchSuperClass = true;

        getHeaders = getNewReflectionBuilder()
                .invokeInstanceMethod("headers", searchSuperClass)
                .build();

        key = getNewReflectionBuilder()
                .invokeInstanceMethod("key", searchSuperClass)
                .build();

        value = getNewReflectionBuilder()
                .invokeInstanceMethod("value", searchSuperClass)
                .build();
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
    public String unmarshalTransactionContext(Object invokedObject, String className, String methodName,
                                              Object[] paramValues, ISDKUserContext context) throws ReflectorException {
        String result = null;
        try {
            if (paramValues != null && paramValues.length > 0) {
                Object o = paramValues[2];
                if (o == null){

                }
                else {
                    try{
                            Object v = value.execute(o.getClass().getClassLoader(), o);
                            Object[] headers = getHeaders.execute(o.getClass().getClassLoader(), o);
                            for (int i =0;i<headers.length;i++) {
                                Object header = headers[i];
                                String keyString = key.execute(header.getClass().getClassLoader(), header);
                                if (keyString.equals(ITransactionDemarcator.APPDYNAMICS_TRANSACTION_CORRELATION_HEADER)) {
                                    byte[] byteValue = value.execute(header.getClass().getClassLoader(), header);
                                    result = new String(byteValue);
                                }
                            }
                        }catch(Exception ex){
                            getLogger().warn("message", ex);
                        }
                    }
            }

        } catch (Exception et) {
            getLogger().warn("message", et);
        }
        return result;
    }

    @Override
    public String getBusinessTransactionName(Object invokedObject, String className,
                                             String methodName, Object[] paramValues, ISDKUserContext context) throws ReflectorException {
        String result = null;
        if (identifyBt)
            result = new String("Kafka Receive");
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
}