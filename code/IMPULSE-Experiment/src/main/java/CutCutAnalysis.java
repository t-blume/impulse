import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

public class CutCutAnalysis {
    //counter
    private static int abstractDocuments = 0;
    private static int keywords = 0;
    private static int conceptDocuments = 0;
    private static int keywordorconcepts = 0;
    private static int addedConcepts = 0;
    private static int newConcepts = 0;

    private static int abstracts2 = 0;
    private static int keywords2 = 0;
    private static int concepts2 = 0;
    private static int keywordorconcepts2 = 0;


    private String INDEX;
    private String TYPE;
    private String INPUT;
    private String INPUT2;
    private static HashMap<String, String> doc2Analyse = new HashMap<String, String>();
    private static HashMap<String, String> cut = new HashMap<String, String>();
    private static HashMap<String, String> doc2AnalyseSecond = new HashMap<String, String>();
    private static HashSet<String> doc2AnalyseFirst = new HashSet<>();
    private static HashSet<String> doc2Analyse2 = new HashSet<>();


    private static HashSet<String> id_first = new HashSet<>();
    private static HashSet<String> id_second = new HashSet<>();

    private Map<String, Object> contextURIs = new HashMap<>();


    public CutCutAnalysis(String index, String type, String input, String input2) throws IOException {
        this.INDEX = index;
        this.TYPE = type;
        this.INPUT = input;
        this.INPUT2 = input2;
        loadInput(input, input2);

    }

    private void loadInput(String input, String input2) throws IOException {
        String line;
        BufferedReader reader = new BufferedReader(new FileReader(input));
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split("=", 2);
            if (parts.length >= 2) {
                String key = parts[0];
                String value = parts[1];
                doc2Analyse.put(key, value);
            } else {
                System.out.println("ignoring line: " + line);
            }
        }


        String ID_first = null;
        String ID_second = null;


        for (String key : doc2Analyse.keySet()) {
            int begin = doc2Analyse.get(key).indexOf("ID_first") + 9;
            int end = begin + 20;
            ID_first = doc2Analyse.get(key).substring(begin, end);


            doc2AnalyseFirst.add(ID_first);


        }


        String line2;
        BufferedReader reader2 = new BufferedReader(new FileReader(input2));
        while ((line2 = reader2.readLine()) != null) {
            String[] parts = line2.split("=", 2);
            if (parts.length >= 2) {
                String key = parts[0];
                String value = parts[1];
                doc2AnalyseSecond.put(key, value);
            } else {
                System.out.println("ignoring line: " + line2);
            }
        }
        reader2.close();
        for (String key : doc2AnalyseSecond.keySet()) {
            int begin = doc2AnalyseSecond.get(key).indexOf("ID_first") + 9;
            int end = begin + 20;
            ID_second = doc2AnalyseSecond.get(key).substring(begin, end);


            doc2Analyse2.add(ID_second);


        }


    }


    public void analyse() throws UnknownHostException {
        Client client = new PreBuiltTransportClient(
                Settings.builder().put("client.transport.sniff", true)
                        .put("cluster.name", "elasticsearch").build())
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

        Iterator<String> iter = id_first.iterator();
        Iterator<String> iter2 = id_second.iterator();


        doc2Analyse2.forEach(x -> {
            Map<String, Object> source = new HashMap<String, Object>();

            GetResponse response = client.prepareGet("kummuliert_context", TYPE, x).get();


            source = response.getSourceAsMap();

            String title = (String) source.get("title");
            String ID_first = (String) response.getId();


            cut.put(title, "kummuliert_context");


        });

        System.out.println("Items from first Cut: " + cut.size());


        doc2AnalyseFirst.forEach(x -> {

            Map<String, Object> source = new HashMap<String, Object>();

            GetResponse response = client.prepareGet("kummuliert_context_type", TYPE, x).get();


            source = response.getSourceAsMap();

            String title = (String) source.get("title");
            String ID_first = (String) response.getId();

            if (!cut.containsKey(title))
                cut.put(title, "kummuliert_context_type");


        });

//        for (Map.Entry<String, String> entry : cut.entrySet()) {
//            System.out.println(entry.getKey() + " / " + entry.getValue());
//
//
//        }
        System.out.println("Total items: " + cut.size());
    }


}
