import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class LowLevelElasticsearch {

    private static final RequestOptions COMMON_OPTIONS;

    //addHeader is for headers that are required for authorization or to work with a proxy in front of Elasticsearch.
    // There is no need to set the Content-Type header because the client will automatically set that from the HttpEntity attached to the request.
    //addHeader is for headers that are required for authorization or to work with a proxy in front of Elasticsearch. There is no need to set the Content-Type header because the client will automatically set that from the HttpEntity attached to the request.
    //You can also customize the response consumer used to buffer the asynchronous responses.
    // The default consumer will buffer up to 100MB of response on the JVM heap. If the response is larger then the request will fail.
    // You could, for example, lower the maximum size which might be useful if you are running in a heap constrained environment like
    // the example above.
    static {
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.addHeader("Authorization", "Bearer " + TOKEN);
        builder.setHttpAsyncResponseConsumerFactory(new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(30 * 1024
                * 1024 * 1024) {
        });
        COMMON_OPTIONS = builder.build();
    }


    private void restClient() {
        RestClient restClient = RestClient.builder(
                new HttpHost("localhost", 9200, "http"),
                new HttpHost("localhost", 9201, "http")).build();

        try {
            restClient.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        RestClientBuilder builder = RestClient.builder(
                new HttpHost("localhost", 9200, "http")
        );
        Header[] defaultHeaders = new Header[]{new BasicHeader("header", "value")};
        builder.setDefaultHeaders(defaultHeaders);
        //Set a listener that gets notified every time a node fails, in case actions need to be taken. Used internally when sniffing on failure is enabled.
        builder.setFailureListener(new RestClient.FailureListener() {
            @Override
            public void onFailure(Node node) {
                super.onFailure(node);
            }
        });
        //Set the node selector to be used to filter the nodes the client will send requests
        //
        //to among the ones that are set to the client itself. This is useful for instance to prevent sending requests to dedicated master
        // nodes when sniffing is enabled. By default the client sends requests to every configured nod
        builder.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);

        //Set a callback that allows to modify the default request configuration
        // (e.g. request timeouts, authentication, or anything that the org.apache.http.client.config.RequestConfig.Builder allows to set)
        builder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            @Override
            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                return requestConfigBuilder.setSocketTimeout(10000);
            }
        });
        //
        //Set a callback that allows to modify the http client configuration (e.g. encrypted communication over ssl, or anything that
        // the org.apache.http.impl.nio.client.HttpAsyncClientBuilder allows to set)
        builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setProxy(new HttpHost("proxy", 9000, "http"));
            }
        });
    }

    private RestClient getRestClient() {
        RestClient restClient = RestClient.builder(
                new HttpHost("localhost", 9200, "http"),
                new HttpHost("localhost", 9201, "http")).build();

        try {
            restClient.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return restClient;
    }


    //Synchronize perform request
    private void performingRequest() {
        Request request = new Request("GET", "/");
        //You can add request parameters to the request object:
        request.addParameter("pretty", "true");

        //you can set the body of the request to any HttpEntity
        request.setEntity(new StringEntity(
                "{\"json\":\"text\"}", ContentType.APPLICATION_JSON
        ));

        try {
            Response response = getRestClient().performRequest(request);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void performAsyncRequest() {
        //
        //The HTTP method (GET, POST, HEAD, etc)
        //
        //
        //The endpoint on the server
        //
        //
        //Handle the response
        //
        //
        //Handle the failure
        Request request = new Request("GET", "/");
        //Once you’ve created the singleton you can use it when making requests:
        request.setOptions(COMMON_OPTIONS);
        //WE can also customize these options on a per request basis.
        RequestOptions.Builder options = COMMON_OPTIONS.toBuilder();
        options.addHeader("cats", "knock things off of other things");
        request.setOptions(options);

        Cancellable cancellable = getRestClient().performRequestAsync(request, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {

            }

            @Override
            public void onFailure(Exception e) {

            }
        });

    }

    ///Multiple parallel asynchronous actions
    //The client is quite happy to execute many actions in parallel.
    // The following example indexes many documents in parallel. In a real world scenario you’d probably want to use the _bulk API instead,
    // but the example is illustrative.
    private void performMultipleAsyncActions(ArrayList documents) {
        final CountDownLatch latch = new CountDownLatch(documents.length);
        for (int i = 0; i < documents.length,i++){
            Request request = new Request("PUT", "/posts/doc/" + i);
            //let's assume that the documents are stored in an HttpEntity array
            request.setEntity(documents[i]);
            getRestClient().performRequestAsync(request, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    latch.countDown();
                }
            });
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }


    //Cancelling asynchronous requestsedit
    //The performRequestAsync method returns a Cancellable that exposes a single public method called cancel.
    // Such method can be called to cancel the on-going request.
    // Cancelling a request will result in aborting the http request through the underlying http client.
    // On the server side, this does not automatically translate to the execution of that request being cancelled,
    // which needs to be specifically implemented in the API itself.
    //
    //The use of the Cancellable instance is optional and you can safely ignore this if you don’t need it.
    // A typical usecase for this would be using this together with frameworks like Rx Java or the Kotlin’s suspendCancellableCoRoutine.
    // Cancelling no longer needed requests is a good way to avoid putting unnecessary load on Elasticsearch.

    private void cancelAsyncRequest() {
        Request request = new Request("GET", "/posts/_search");
        Cancellable cancellable = getRestClient().performRequestAsync(
                request,
                new ResponseListener() {
                    @Override
                    public void onSuccess(Response response) {

                    }

                    @Override
                    public void onFailure(Exception exception) {

                    }
                }
        );
        cancellable.cancel();
    }


}
