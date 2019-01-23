import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

public class PLDExtract {
    private int counter = 0;
    private static String INDEX;
    private static String TYPE;
    private static String parameter;
    private Map<String, Object> contextURIs = new HashMap<>();
    private String output;

    public PLDExtract(String index, String type, String output) {
        this.INDEX = index;
        this.TYPE = type;
        this.output = output;

    }

    public void loadInput() throws UnknownHostException {

        Client client = new PreBuiltTransportClient(
                Settings.builder().put("client.transport.sniff", true)
                        .put("cluster.name", "elasticsearch").build())
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));


        SearchResponse scrollResponse = client.prepareSearch(INDEX).setTypes(TYPE)
                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(new TimeValue(60000))
                .setQuery(matchAllQuery())
                .setSize(100)
                .get();

        Map<String, Object> source = new HashMap<String, Object>();
        do {
            for (SearchHit hit : scrollResponse.getHits().getHits()) {
                source = hit.getSourceAsMap();
                List<String> sourceURL;


                // contextURIs.put((String)source.get("abstract"), "dummy");
                if ((List) source.get("identifier_url") != null) {

                    sourceURL = (List) source.get("identifier_url");
                    for (int i = 0; i < sourceURL.size(); i++) {
                        contextURIs.put(sourceURL.get(i), "dummy");
                    }

                }
            }

            scrollResponse = client.prepareSearchScroll(scrollResponse.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
        }
        while (scrollResponse.getHits().getHits().length != 0); // Zero hits mark the end of the scroll and the while loop.


        extractPLDs();
//        System.out.println(counter);
    }

    public void extractPLDs() {

        Map<String, Integer> countPLDs = new HashMap<>();
        Pattern p = Pattern.compile("([a-z]*\\.)?[a-z]*\\.([a-z]{2}\\.[a-z]{2}|[a-z]{2,3})\\/");
        int counter = 0;

        for (String URI : contextURIs.keySet()) {


            Matcher m = p.matcher(URI);
            if (m.find()) {
                String pld = m.group();
                if (pld.startsWith("www."))
                    pld = pld.substring(4);
                countPLDs.merge(pld.substring(0, pld.length() - 1), 1, (OLD, NEW) -> OLD + NEW);
            } else
                System.out.println("error: " + URI);


        }


        countPLDs.forEach((K, V) -> System.out.println(K + ";" + V));


        // Write JSON to File

        try (FileWriter file = new FileWriter(output)) {
            countPLDs.forEach((K, V) -> {
                try {
                    file.write(K + " : " + V + "\n");

                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            System.out.println("Finished... Successfully Copied JSON Object to File...");
        } catch (IOException e) {
            e.printStackTrace();
        }


    }


    private String getSubDomainName(URL url) {
        return url.getHost();
    }


}



