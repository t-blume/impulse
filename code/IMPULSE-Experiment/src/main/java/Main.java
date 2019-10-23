import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;


/**
 * Created by Sven Lüdeke
 * Doing some Analyse on the ElasticSearch Indices.
 * <p>
 * - Deduplication, delete noisy data
 * - Find cut with EconBIZ
 * - Upload EconBIZ data to index
 * - Check the offline resources of the Own_Crawl Dataset
 */


public class Main {

    private static final String INDEX = "kummuliert_pld_inf";
    private static final String TYPE = "publication";

    private static HashSet<String> indices;

    private static List<String> docs2Delete = new ArrayList<String>();

    private static HashMap<String, Object> dedup = new HashMap<String, Object>();
    private static HashMap<String, Object> first = new HashMap<String, Object>();
    private static HashMap<String, Object> second = new HashMap<String, Object>();
    private static List<String> duplicates = new ArrayList<String>();


    public static void main(String[] args) throws IOException {
        indices = new HashSet<>();

        CutAnalysis cutAnalysis = new CutAnalysis(INDEX, TYPE, "C:\\Users\\svenl\\Git\\bibliographic-metadata\\code\\Experiment-Analyse-Tool\\Cuts\\old_Cuts\\econbiz_pld_inf.txt");

        cutAnalysis.analyse();
//        indices.add("bibo_context");
//        indices.add("bibo_context_inf");
//        indices.add("bibo_pld");
//        indices.add("bibo_pld_inf");
//        indices.add("bibo_context_type");
//        indices.add("bibo_pld_type");
//        indices.add("bibo_context_type_inf");
//        indices.add("bibo_pld_type_inf");
//        indices.add("swrc_context");
//        indices.add("swrc_context_inf");
//        indices.add("swrc_pld");
//        indices.add("swrc_pld_inf");
//        indices.add("swrc_context_type");
//        indices.add("swrc_pld_type");
//        indices.add("swrc_context_type_inf");
//        indices.add("swrc_pld_type_inf");
//        indices.add("dcterms_context");
//        indices.add("dcterms_context_inf");
//        indices.add("dcterms_pld");
//        indices.add("dcterms_pld_inf");
//        indices.add("dcterms_context_type");
//        indices.add("dcterms_pld_type");
//        indices.add("dcterms_context_type_inf");
//        indices.add("dcterms_pld_type_inf");
        indices.add("kummuliert_context,C:\\Users\\svenl\\Git\\bibliographic-metadata\\code\\Experiment-Analyse-Tool\\Cuts\\old_Cuts\\econbiz-kummuliert_context_exact.txt");
        indices.add("kummuliert_context,C:\\Users\\svenl\\Git\\bibliographic-metadata\\code\\Experiment-Analyse-Tool\\Cuts\\old_Cuts\\econbiz-kummuliert_context_fuzzy.txt");
        indices.add("kummuliert_pld,C:\\Users\\svenl\\Git\\bibliographic-metadata\\code\\Experiment-Analyse-Tool\\Cuts\\old_Cuts\\econbiz-kummuliert_pld_exact.txt");
        indices.add("kummuliert_pld,C:\\Users\\svenl\\Git\\bibliographic-metadata\\code\\Experiment-Analyse-Tool\\Cuts\\old_Cuts\\econbiz-kummuliert_pld_fuzzy.txt");
        indices.add("kummuliert_context_inf,C:\\Users\\svenl\\Git\\bibliographic-metadata\\code\\Experiment-Analyse-Tool\\Cuts\\old_Cuts\\econbiz-kummuliert_context_inf_exact.txt");
        indices.add("kummuliert_context_inf,C:\\Users\\svenl\\Git\\bibliographic-metadata\\code\\Experiment-Analyse-Tool\\Cuts\\old_Cuts\\econbiz-kummuliert_context_inf_fuzzy.txt");
        indices.add("kummuliert_pld_inf,C:\\Users\\svenl\\Git\\bibliographic-metadata\\code\\Experiment-Analyse-Tool\\Cuts\\old_Cuts\\econbiz-kummuliert_pld_inf_exact.txt");
        indices.add("kummuliert_pld_inf,C:\\Users\\svenl\\Git\\bibliographic-metadata\\code\\Experiment-Analyse-Tool\\Cuts\\old_Cuts\\econbiz-kummuliert_pld_inf_fuzzy.txt");
//        indices.add("kummuliert_context_type,C:\\Users\\svenl\\Git\\bibliographic-metadata\\code\\Experiment-Analyse-Tool\\Cuts\\old_Cuts\\econbiz-kummuliert_context_exact.txt");
//        indices.add("kummuliert_pld_type");
//        indices.add("kummuliert_context_type_inf");
//         indices.add("kummuliert_pld_type_inf");
//            indices.add("own_bibo_context");
//            indices.add("own_bibo_context_type");
//            indices.add("own_bibo_context_inf");
//            indices.add("own_bibo_context_inf_type");
//            indices.add("own_bibo_pld");
//            indices.add("own_bibo_pld_type");
//            indices.add("own_bibo_pld_inf");
//            indices.add("own_bibo_pld_inf_type");


//
//
//        System.out.println("Start deduplication... " + INDEX);
//
//        Boolean fuzzy = false;
//
//        Deduplication deduplication = new Deduplication(INDEX, TYPE, fuzzy);
//
//        deduplication.deduplication();
//        indices.forEach(x -> {
//
//                    try {
//                        deleteAll(x, TYPE);
//                    } catch (UnknownHostException e) {
//                        e.printStackTrace();
//                    }
//
//                }
//        );
//        deleteAll(INDEX, TYPE);
//        JSONUpload JSONUpload = new JSONUpload(INDEX, TYPE);
//
//            JSONUpload.upload("E:\\Bachelorarbeit\\paper\\paperexp\\bibo\\normal", "other");

////
//        System.out.println("Start analysis... " + INDEX);
//        Analyse analyse = new Analyse(INDEX, TYPE);
//
//        analyse.analyse();

//        indices.forEach(X -> {
//                    try {
//                        System.out.println("Start analysis... " + X);
//                        Analyse analyse = new Analyse(X, TYPE);
//
//                        analyse.analyse();
//
//                        System.out.println("Analysis finsihed sucessfully! Moving on to next Index..." );
//                    } catch (UnknownHostException e) {
//                        e.printStackTrace();
//                    }
//                }
//        );


//          String output = "C:\\Users\\svenl\\Git\\bibliographic-metadata\\code\\Experiment-Analyse-Tool\\PLD_Analyse\\econbiz.txt";
////        String parameter = "keywords";
//        PLDExtract pldExtract = new PLDExtract(INDEX,TYPE, output);
////
//        pldExtract.loadInput();
//        SplitSeedlist splitSeedlist = new SplitSeedlist(INDEX,TYPE, output);
//
//        splitSeedlist.loadInput();
//
//

        System.setErr(new PrintStream(new OutputStream() {
            @Override
            public void write(int b) {
            }
        }));

//        indices.forEach(X -> {
//                    try {
////                        System.out.println("Start analysis... " + X);
//
//                        String[] parts = X.split(",");
//                        String part1 = parts[0]; // 004
//                        String part2 = parts[1]; // 034556
//
//
//                        CutAnalysis cutAnalysis = new CutAnalysis(part1, TYPE, part2);
//
//                        cutAnalysis.analyse();
//
//                        System.out.println("Analysis finsihed sucessfully! Moving on to next Index...");
//                    } catch (UnknownHostException e) {
//                        e.printStackTrace();
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                }
//        );


//
//        CutCutAnalysis cutCutAnalysis = new CutCutAnalysis(INDEX, TYPE, "C:\\Users\\svenl\\Git\\bibliographic-metadata\\code\\Experiment-Analyse-Tool\\Cuts\\econbiz_context_type.txt", "C:\\Users\\svenl\\Git\\bibliographic-metadata\\code\\Experiment-Analyse-Tool\\Cuts\\econbiz_context.txt");
//
//        cutCutAnalysis.analyse();


        // System.out.println(countLinesOld("E:\\Bachelorarbeit\\paper\\data\\swrc\\data.nq-2"));

//        getSourceURL getSourceURL = new getSourceURL(INDEX, TYPE, "C:\\Users\\svenl\\Git\\Bachelorarbeit_Sven_Luedeke2\\ElasticSearchDeduplication\\cut.file");
//        getSourceURL.getSourceURL();

        // init();
//           FindCut findCut = new FindCut(INDEX, TYPE);
//
//             findCut.findeSchnitt();

//

//
//        for (int i = 1; i < 22; i++) {
//            JSONUpload JSONUpload = new JSONUpload(INDEX, TYPE);
//
//            JSONUpload.upload("E:\\Bachelorarbeit\\econbiz-json_2018-07-06\\" + i + ".json");
//
//        }


    }

    private static void deleteAll(String index, String type) throws UnknownHostException {

        Client client = new PreBuiltTransportClient(
                Settings.builder().put("client.transport.sniff", true)
                        .put("cluster.name", "elasticsearch").build())
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));


        //SearchResponse searchResponse = client.prepareSearch("testindex3").setTypes("publication").setQuery(existsQuery("title")).get();
        SearchResponse scrollResponse = client.prepareSearch(index).setTypes(type)
                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(new TimeValue(60000))
                .setQuery(matchAllQuery())
                .setSize(100)
                .get();

        Map<String, Object> docFromIndex = new HashMap<String, Object>();
        do {
            for (SearchHit hit : scrollResponse.getHits().getHits()) {

                docs2Delete.add(hit.getId());


            }
            scrollResponse = client.prepareSearchScroll(scrollResponse.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
        }
        while (scrollResponse.getHits().getHits().length != 0); // Zero hits mark the end of the scroll and the while loop.
        updateIndex(index);

    }


    private static void updateIndex(String index) throws UnknownHostException {


        Client client = new PreBuiltTransportClient(
                Settings.builder().put("client.transport.sniff", true)
                        .put("cluster.name", "elasticsearch").build())
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));


        BulkRequestBuilder bulkRequest = client.prepareBulk();

        Iterator<String> iter2Delete = docs2Delete.iterator();

        while (iter2Delete.hasNext()) {


            bulkRequest.add(client.prepareDelete()
                    .setIndex(index)
                    .setType(TYPE)
                    .setId(iter2Delete.next()));
        }


        if ((bulkRequest != null) && (!bulkRequest.request().requests().isEmpty())) {
            BulkResponse bulkResponse = bulkRequest.execute().actionGet();
            if (bulkResponse.hasFailures()) {
                System.out.println("Elasticsearch cleaning failed");
            } else {
                System.out.println("Deduplication " + index + " finished! ... " + " deleted " + bulkResponse.getItems().length + " documents");
            }
        } else {
            System.out.println("Elasticsearch cleaning" + INDEX + "finished without deleting any documents");
        }
    }

    public static int countLinesOld(String filename) throws IOException {
        InputStream is = new BufferedInputStream(new FileInputStream(filename));
        try {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally {
            is.close();
        }
    }
}



