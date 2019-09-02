package main.java.output.implementation;

import main.java.output.interfaces.IJsonSink;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;


public class Elastify extends Thread implements IJsonSink {
    private static final Logger logger = LogManager.getLogger(FileJSONSink.class.getSimpleName());

    private static final String BASE_URL = "localhost";
    private static final int PORT = 9300;

    private String index;
    private String type;
    private long successfullyAdded = 0;
    private long errors = 0;


    //Connection is shared among all Elastify objects
    private static RestHighLevelClient client;


    /**
     * @param index = moving
     * @param type  = publication
     * @throws UnknownHostException
     * @throws IOException
     */

    public Elastify(String index, String type) throws UnknownHostException, IOException {
        this.index = index;
        this.type = type;
        initClient();
    }

    @Override
    public boolean export(String jsonString) {
        return false;
    }

    @Override
    public int bulkExport(List<String> jsonStrings) {
        BulkRequest bulkRequest = new BulkRequest();
        // either use client#prepare, or use Requests# to directly build index/delete requests
        if (jsonStrings != null && !jsonStrings.isEmpty()) {
            for (String jsonString : jsonStrings) {
                //add data to the request
                bulkRequest.add(new IndexRequest(index, type).source(jsonString));

            }
        }
        //send request normally
        try {
            BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            if (!bulkResponse.hasFailures())
                successfullyAdded += bulkRequest.numberOfActions();
            else
                errors += bulkRequest.numberOfActions();

            return bulkResponse.getItems().length;
        } catch (NoNodeAvailableException | IOException e) {
            logger.error("["+this.getClass().getSimpleName()+"] " + e.getLocalizedMessage());
            return 0;
        }
    }

    @Override
    public boolean close() {
        return false;
    }


    //    /**
//     * Create singleton instance
//     *
//     * @throws UnknownHostException
//     */
    private static void initClient() throws UnknownHostException, IOException {
        if (client == null) {
            client = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost("localhost", 9200, "http"),
                            new HttpHost("localhost", 9201, "http")));
        }
    }


}
