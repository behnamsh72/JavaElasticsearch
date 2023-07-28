import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jsonb.JsonbJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;

public class Connecting {
    // URL and API key
    private String serverUrl = "https://localhost:9200";
    private String apiKey = "VnVhQ2ZHY0JDZGJrU...";

    public ElasticsearchClient elasticsearchClient() {
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

    private void firstRequest() {
        ElasticsearchClient esClient = elasticsearchClient();

        SearchResponse<Product> search = esClient.search(s ->
                        s.index("products")
                                .query(q ->
                                        q.term(t -> t.field("name")
                                                .value(v -> v.stringValue("bicycle")))),
                Product.class);

        for (Hit<Product> hit : search.hits().hits()) {
            processProduct(hit.source());
        }

    }

}
