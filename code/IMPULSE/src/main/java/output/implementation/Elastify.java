package main.java.output.implementation;

import com.fasterxml.jackson.databind.ObjectMapper;
import main.java.output.interfaces.IJsonSink;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

import static main.java.utils.MainUtils.loadJSON;


public class Elastify extends Thread implements IJsonSink {
    private static final Logger logger = LogManager.getLogger(FileJSONSink.class.getSimpleName());

    private static final String BASE_URL = "localhost";
    private static final int PORT = 9300;

    private String index;
    private String type;
    private long successfullyAdded = 0;
    private long errors = 0;


    //Connection is shared among all Elastify objects
    private static Client client;

    public static void main(String[] args) throws IOException {

        Elastify elastify = new Elastify("moving", "publication");
        List<String> jsonStrings = loadJSON("export_testQuery1_01.json");
        Stack<String> JSONObjects = new Stack<>();
        Iterator<String> iter = jsonStrings.iterator();
        ObjectMapper mapper = new ObjectMapper();
        while (iter.hasNext()) {
            String json = mapper.writeValueAsString(iter.next());
            JSONObjects.push(json);
        }
       // elastify.bulkUpload(JSONObjects);
    }


    /**
     * @param index = moving
     * @param type  = publication
     * @throws UnknownHostException
     * @throws NoNodeAvailableException
     * @throws IOException
     */

    public Elastify(String index, String type) throws UnknownHostException, NoNodeAvailableException, IOException {
        this.index = index;
        this.type = type;
       // initClient();
    }

    @Override
    public boolean export(String jsonString) {
        return false;
    }

    @Override
    public int bulkExport(List<String> jsonStrings) {
        return 0;
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
//    private static void initClient() throws UnknownHostException, NoNodeAvailableException, IOException {
//        if (client == null) {
//            //use all available nodes, switch if needed
//            Settings settings = Settings.settingsBuilder().put("client.transport.sniff", true).build();
//            //build Transport client instance (uses thread pool internally)
//            client = TransportClient.builder().settings(settings).build()
//                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(BASE_URL), PORT));
//            //check if Elasticsearch is running
//            List<DiscoveryNode> nodes = ((TransportClient) client).connectedNodes();
//
//            if (nodes.isEmpty())
//                throw new NoNodeAvailableException("No Elasticsearch node available!");
//
//        }
//    }
//
//
//    @Override
//    public boolean export(String jsonString) {
//        try {
//            return upload(jsonString).isCreated();
//        } catch (IOException e) {
//            return false;
//        }
//    }
//
//    @Override
//    public int bulkExport(List<String> jsonStrings) {
//        try {
//           return bulkUpload(jsonStrings).getItems().length;
//        } catch (IOException e) {
//            return 0;
//        }
//    }
//
//    @Override
//    public boolean close() {
//        logger.info("Successfully added " + successfullyAdded + "/" + (successfullyAdded + errors) +
//                " documents to " + BASE_URL + ":" + PORT + "/" + index + "/" + type);
//        client.close();
//        return true;
//    }
//
//
//    /**
//     * @param jsonObjects
//     * @return
//     * @throws IOException
//     */
//    public BulkResponse bulkUpload(List<String> jsonObjects) throws IOException, JsonParseException, JsonMappingException {
//        if (jsonObjects == null || jsonObjects.isEmpty())
//            return null;
//
//        BulkRequestBuilder bulkRequest = client.prepareBulk();
//        // either use client#prepare, or use Requests# to directly build index/delete requests
//        Iterator<String> jsonIterator = jsonObjects.iterator();
//
//        while (jsonIterator.hasNext()) {
//            String json = jsonIterator.next();
//            try {
//                //add data to the request
//                bulkRequest.add(client.prepareIndex(index, type)
//                        .setSource(json));
//            } catch (OutOfMemoryError e) {
//                //sending intermediate Request
//                logger.debug("Sending intermediate chunk of " + bulkRequest.numberOfActions() + " documents!");
//                BulkResponse intermediateResponse = sendBulk(bulkRequest);
//                if (!intermediateResponse.hasFailures())
//                    logger.debug("Successfully uploaded a chunk of " + intermediateResponse.getItems().length + " documents!");
//
//                bulkRequest = client.prepareBulk();
//                try {
//                    //add data to the request
//                    upload(json);
//                    successfullyAdded++;
//                    //TODO: false for isCreated does not mean not is is not added
//                } catch (OutOfMemoryError e1) {
//                    //StringBuilder can throw java.lang.OutOfMemoryError Exception
//                    logger.debug("Document=\"" + json + "\" too large to process individually!!");
//                    errors++;
//                }
//            }
//        }
//
//        //send request normally
//        BulkResponse bulkResponse = sendBulk(bulkRequest);
//        return bulkResponse;
//    }
//
//    private BulkResponse sendBulk(BulkRequestBuilder bulkRequest) {
//        if (bulkRequest != null && bulkRequest.numberOfActions() > 0) {
//            //send request
//            try {
//                BulkResponse bulkResponse = bulkRequest.get();
//                if (!bulkResponse.hasFailures())
//                    successfullyAdded += bulkRequest.numberOfActions();
//                else
//                    errors += bulkRequest.numberOfActions();
//
//                return bulkResponse;
//            } catch (NoNodeAvailableException e) {
//                System.err.println(e.getLocalizedMessage());
//                return null;
//            }
//        } else return null;
//    }
//
//    public IndexResponse upload(String jsonObject) throws IOException, UnknownHostException, JsonParseException, JsonMappingException {
//        if (jsonObject == null)
//            return null;
//
//
//        //add data to the request
//        IndexRequestBuilder indexRequest = client.prepareIndex(index, type).
//                setSource(jsonObject);
//
//        //send request
//        try {
//            IndexResponse response = indexRequest.get();
//            return response;
//        } catch (NoNodeAvailableException e) {
//            System.err.println(e.getLocalizedMessage());
//            return null;
//        }
//    }


}
