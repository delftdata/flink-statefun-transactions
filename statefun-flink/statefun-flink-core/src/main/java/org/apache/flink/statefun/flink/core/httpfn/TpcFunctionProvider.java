package org.apache.flink.statefun.flink.core.httpfn;

import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import org.apache.flink.statefun.flink.core.common.ManagingResources;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClient;
import org.apache.flink.statefun.flink.core.tpcfn.TpcFunction;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.Map;

import static org.apache.flink.statefun.flink.core.httpfn.OkHttpUnixSocketBridge.configureUnixDomainSocket;

@NotThreadSafe
public class TpcFunctionProvider implements StatefulFunctionProvider, ManagingResources {
    private final Map<FunctionType, TpcFunctionSpec> supportedTypes;

    /** lazily initialized by {code buildHttpClient} */
    @Nullable private OkHttpClient sharedClient;

    private volatile boolean shutdown;

    public TpcFunctionProvider(Map<FunctionType, TpcFunctionSpec> supportedTypes) {
        this.supportedTypes = supportedTypes;
    }

    @Override
    public TpcFunction functionOfType(FunctionType type) {
        TpcFunctionSpec spec = supportedTypes.get(type);
        if (spec == null) {
            throw new IllegalArgumentException("Unsupported type " + type);
        }
        return new TpcFunction(
                spec.maxNumBatchRequests(),
                buildHttpClient(spec));
    }

    public TpcFunctionSpec getFunctionSpec(FunctionType type) {
        return supportedTypes.get(type);
    }

    private RequestReplyClient buildHttpClient(TpcFunctionSpec spec) {
        if (sharedClient == null) {
            sharedClient = OkHttpUtils.newClient();
        }
        OkHttpClient.Builder clientBuilder = sharedClient.newBuilder();
        clientBuilder.callTimeout(spec.maxRequestDuration());
        clientBuilder.connectTimeout(spec.connectTimeout());
        clientBuilder.readTimeout(spec.readTimeout());
        clientBuilder.writeTimeout(spec.writeTimeout());

        final HttpUrl url;
        if (spec.isUnixDomainSocket()) {
            UnixDomainHttpEndpoint endpoint = UnixDomainHttpEndpoint.parseFrom(spec.endpoint());

            url =
                    new HttpUrl.Builder()
                            .scheme("http")
                            .host("unused")
                            .addPathSegment(endpoint.pathSegment)
                            .build();

            configureUnixDomainSocket(clientBuilder, endpoint.unixDomainFile);
        } else {
            url = HttpUrl.get(spec.endpoint());
        }
        return new HttpRequestReplyClient(url, clientBuilder.build(), () -> shutdown);
    }

    @Override
    public void shutdown() {
        shutdown = true;
        OkHttpUtils.closeSilently(sharedClient);
    }
}
