import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;

public class JSONUpload {
    private String INDEX;
    private String TYPE;

    public JSONUpload(String index, String type) {
        this.INDEX = index;
        this.TYPE = type;
    }

    public void upload(String path) throws IOException {

        Client client = new PreBuiltTransportClient(
                Settings.builder().put("client.transport.sniff", true)
                        .put("cluster.name", "elasticsearch").build())
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));


        BulkRequestBuilder bulkRequest = client.prepareBulk();
        try {
            BufferedReader br = new BufferedReader(new FileReader(path));
            String line;
            while ((line = br.readLine()) != null) {
                // System.out.println(line);
                JSONParser parser = new JSONParser();
                try {

                    Object obj = parser.parse(line);

                    JSONObject jsonObject = (JSONObject) obj;
                    // System.out.println(jsonObject);

                    bulkRequest.add(client.prepareIndex(INDEX, TYPE)
                            .setSource(jsonObject));

                } catch (ParseException e) {
                    e.printStackTrace();
                }


            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if ((bulkRequest != null) && (!bulkRequest.request().requests().isEmpty())) {
            BulkResponse bulkResponse = bulkRequest.execute().actionGet();
            if (bulkResponse.hasFailures()) {
                System.out.println("Elasticsearch indexing failed");
            } else {
                System.out.println("Indexing finished! Indexed " +
                        bulkResponse.getItems().length + " documents");
            }
        }
    }
}
