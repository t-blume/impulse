package connector;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
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
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

/**
 * Simple Uploader class that functions as uploading interface to Elasticsearch.
 */
public class ElasticsearchClient {
    private static final Logger logger = LogManager.getLogger(ElasticsearchClient.class.getSimpleName());

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
                if (jsonObject.containsKey("title"))
                    jsonObjects.add(jsonObject);
            } catch (ParseException e) {
                //increase error counter
                result[1]++;
            }

            if (jsonObjects.size() >= bulkSize) {
                logger.info("Sending " + jsonObjects.size() + " objects...");
                int[] tmpResult = bulkUpload(jsonObjects);
                jsonObjects = new LinkedList<>();
                result[0] += tmpResult[0];
                result[1] += tmpResult[1];
            }
            counter++;
        }
        if (jsonObjects.size() >= 0) {
            logger.info("Sending last " + jsonObjects.size() + " objects...");
            int[] tmpResult = bulkUpload(jsonObjects);
            result[0] += tmpResult[0];
            result[1] += tmpResult[1];
        }
        logger.info("Uploaded a total of " + counter + " documents.");
        return result;
    }

    public int[] bulkUpload(Collection<JSONObject> jsonObjects) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        int[] result = new int[]{0, 0};
        if (jsonObjects != null)
            jsonObjects.forEach(jsonObject ->
                    bulkRequest.add(new IndexRequest(index).source(jsonObject)));


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

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        if (title != null)
            searchSourceBuilder.query(QueryBuilders.matchPhraseQuery("title", title));  //.moreLikeThisQuery(new String[]{"title", title})
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
        if (scrollID != null) {
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollID);
            scrollRequest.scroll(TimeValue.timeValueSeconds(30));
            SearchResponse searchScrollResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollID = searchScrollResponse.getScrollId();
            return searchScrollResponse.getHits();

        } else {
            logger.error("No search context!");
            return null;
        }
    }

    public boolean releaseScrollContext() throws IOException {
        ClearScrollRequest request = new ClearScrollRequest();
        request.addScrollId(scrollID);
        ClearScrollResponse response = client.clearScroll(request, RequestOptions.DEFAULT);
        return response.isSucceeded();
    }

    public boolean exists() throws IOException {
        GetIndexRequest request = new GetIndexRequest(index);
        return client.indices().exists(request, RequestOptions.DEFAULT);
    }

    public boolean delete(String id) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(index, id);
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        return deleteResponse.getShardInfo().getSuccessful() > 0;
    }


    public void update(String id, Map<String, Object> jsonMap) throws IOException {
        UpdateRequest request = new UpdateRequest(index, id)
                .doc(jsonMap);
        client.update(request, RequestOptions.DEFAULT);
    }
}
