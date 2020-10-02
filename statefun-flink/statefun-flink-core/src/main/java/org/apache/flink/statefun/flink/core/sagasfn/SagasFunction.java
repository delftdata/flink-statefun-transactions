package org.apache.flink.statefun.flink.core.sagasfn;

import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClient;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SagasFunction implements StatefulFunction {
    private final RequestReplyClient client;
    private final int maxNumBatchRequests;

    private static final Logger LOGGER = LoggerFactory.getLogger(org.apache.flink.statefun.flink.core.sagasfn.SagasFunction.class);

    public SagasFunction(
            int maxNumBatchRequests,
            RequestReplyClient client) {
        this.client = Objects.requireNonNull(client);
        this.maxNumBatchRequests = maxNumBatchRequests;
    }

    @Override
    public void invoke(Context context, Object input) {
        return;
    }
}
