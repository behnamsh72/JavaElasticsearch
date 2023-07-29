import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jsonb.JsonbJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;

import java.io.IOException;


public class Connection {
    // URL and API key
    private String serverUrl = "https://localhost:9200";
    private String apiKey = "VnVhQ2ZHY0JDZGJrU...";

    public ElasticsearchClient getElasticsearchClient() {
        // Create the low-level client

        RestClient restClient = RestClient.builder(HttpHost.create(serverUrl))
                .setDefaultHeaders(new Header[]{
                        new BasicHeader("Authorization", "ApiKey " + apiKey)
                })
                .build();

        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JsonbJsonpMapper());

        // And create the API client
        ElasticsearchClient esClient = new ElasticsearchClient(transport);
        return esClient;
    }

    private ElasticsearchAsyncClient getAsyncElasticsearchClient() {
        // Create the low-level client

        RestClient restClient = RestClient.builder(HttpHost.create(serverUrl))
                .setDefaultHeaders(new Header[]{
                        new BasicHeader("Authorization", "ApiKey " + apiKey)
                })
                .build();

        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JsonbJsonpMapper());
        // Asynchronous non-blocking client

        ElasticsearchAsyncClient asyncClient = new ElasticsearchAsyncClient(transport);
        return asyncClient;
    }

    private void testAsyncClient() {
        getAsyncElasticsearchClient()
                .exists(b -> b.index("products").id("foo"))
                .whenComplete((response, exception) -> {
                    if (exception != null) {
                        logger.error("Failed to index", exception);
                    } else {
                        logger.info("Product exists");
                    }
                });
    }


    private void termQuery() {
        ElasticsearchClient esClient = getElasticsearchClient();

        SearchResponse<Product> search = null;
        try {
            search = esClient.search(s ->
                            s.index("products")
                                    .query(q ->
                                            q.term(t -> t.field("name")
                                                    .value(v -> v.stringValue("bicycle")))),
                    Product.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for (Hit<Product> hit : search.hits().hits()) {
            processProduct(hit.source());
        }
    }

    private void creatingIndex() {
        ElasticsearchClient esClient = getElasticsearchClient();

        try {
            esClient.indices().create(c -> c.index("products"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void indexingDocument() {
        Product product = new Product("bk-1", "City bike", 123.0);
        IndexResponse response = getElasticsearchClient().index(i -> i.index("products")
                .id(product.getSku())
                .document(product));

        logger.info("Indexed with version " + response.version());
    }

    private void gettingDocuments() {
        GetResponse<Product> response = getElasticsearchClient()
                .get(g -> g.index("products")
                                .id("bk-1"),
                        Product.class);
        if (response.found()) {
            Product product = response.source();
            logger.info("Product name " + product.getName());
        } else {
            logger.info("Product not found")
        }
    }


    private void searchingDocument() {
        String searchText = "bike";
        SearchResponse<Product> response = getElasticsearchClient().search(s ->
                        s.index("products")
                                .query(q -> q
                                        .match(t -> t
                                                .field("name")
                                                .query(searchText))),
                Product.class);
    }

    private void updatingDocument() {
        Product product = new Product("bk-1", "City bike", 123.0);
        try {
            getElasticsearchClient().update(u ->
                            u.index("products")
                                    .id("bk-1")
                                    .upsert(product),
                    Product.class
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void deletingDocument() {
        try {
            getElasticsearchClient().delete(d ->
                    d.index("products").id("bk-1"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void deleteIndex() {
        try {
            getElasticsearchClient().indices()
                    .delete(c -> c.index("products"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
