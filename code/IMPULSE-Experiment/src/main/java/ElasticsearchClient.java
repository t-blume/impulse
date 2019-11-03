import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

/**
 * Simple Uploader class that functions as uploading interface to Elasticsearch.
 */
public class ElasticsearchClient {
    private String index;
    private String type;
    private RestHighLevelClient client;
    private int bulkSize;

    private String scrollID;

    public ElasticsearchClient(String index, String type, int bulkSize) {
        this.index = index;
        this.type = type;
        this.bulkSize = bulkSize;
        init();
    }

    public String getIndex() {
        return index;
    }

    public void init() {
        client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")));
    }

    public void close() throws IOException {
        client.close();
    }

    public boolean clear() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest(index);
        AcknowledgedResponse deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
        return deleteIndexResponse.isAcknowledged();
    }

    public int[] bulkUploadFile(String filename) throws IOException {
        return bulkUploadFile(new File(filename));
    }

    public int[] bulkUploadFile(File file) throws IOException {
        JSONParser parser = new JSONParser();
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;
        Collection<JSONObject> jsonObjects = new LinkedList<>();

        int[] result = new int[]{0, 0};

        int counter = 0;
        while ((line = br.readLine()) != null) {
            try {
                JSONObject jsonObject = (JSONObject) parser.parse(line);
                jsonObjects.add(jsonObject);
            } catch (ParseException e) {
                //increase error counter
                result[1]++;
            }

            if (jsonObjects.size() >= bulkSize) {
                System.out.println("Sending " + jsonObjects.size() + " objects...");
                int[] tmpResult = bulkUpload(jsonObjects);
                jsonObjects = new LinkedList<>();
                result[0] += tmpResult[0];
                result[1] += tmpResult[1];
            }
            counter++;
        }
        if (jsonObjects.size() >= 0) {
            System.out.println("Sending last " + jsonObjects.size() + " objects...");
            int[] tmpResult = bulkUpload(jsonObjects);
            result[0] += tmpResult[0];
            result[1] += tmpResult[1];
        }
        System.out.println("Counter: " + counter);
        return result;
    }

    public int[] bulkUpload(Collection<JSONObject> jsonObjects) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        int[] result = new int[]{0, 0};
        if (jsonObjects != null)
            jsonObjects.forEach(jsonObject ->
                    bulkRequest.add(new IndexRequest(index, type).source(jsonObject)));


        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        if (!bulkResponse.hasFailures())
            result[0] += bulkRequest.numberOfActions();
        else
            result[1] += bulkRequest.numberOfActions();

        return result;
    }

    public long indexSize() {
        CountRequest countRequest = new CountRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        countRequest.source(searchSourceBuilder);
        try {
            CountResponse countResponse = client.count(countRequest, RequestOptions.DEFAULT);
            return countResponse.getCount();
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }

    public SearchHits search(String title, int size) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(type);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        if(title != null)
            searchSourceBuilder.query(matchQuery("title", title));
        else
            searchSourceBuilder.query(matchAllQuery());

        searchSourceBuilder.size(size);
        searchRequest.source(searchSourceBuilder);
        searchRequest.scroll(TimeValue.timeValueMinutes(1L));
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        scrollID = searchResponse.getScrollId();
        return searchResponse.getHits();
    }

    public SearchHits scroll() throws IOException {
        if(scrollID != null) {
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollID);
            scrollRequest.scroll(TimeValue.timeValueSeconds(30));
            SearchResponse searchScrollResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollID = searchScrollResponse.getScrollId();
            return searchScrollResponse.getHits();

        }else {
            System.err.println("No search context");
            return null;
        }
    }
}
