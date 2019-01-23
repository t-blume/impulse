import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

public class Analyse {
    private String INDEX;
    private String TYPE;
    private static HashMap<String, Object> dedup = new HashMap<String, Object>();
    private static List<String> duplicates = new ArrayList<String>();

    private int counterAbstract = 0;
    private int counterDoc = 0;
    private int counterConcepts = 0;
    private int counterKeywords = 0;
    private int counterKeywordsConceptsAbstract = 0;

    private List<Integer> avgConcept = new ArrayList<>();
    private List<Integer> avgKeyword = new ArrayList<>();


    public Analyse(String index, String type) {
        this.INDEX = index;
        this.TYPE = type;

    }

    public void analyse() throws UnknownHostException {
        Client client = new PreBuiltTransportClient(
                Settings.builder().put("client.transport.sniff", true)
                        .put("cluster.name", "elasticsearch").build())
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));


        //SearchResponse searchResponse = client.prepareSearch("testindex3").setTypes("publication").setQuery(existsQuery("title")).get();
        SearchResponse scrollResponse = client.prepareSearch(INDEX).setTypes(TYPE)
                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(new TimeValue(60000))
                .setQuery(matchAllQuery())
                .setSize(100)
                .get();

        // Map<String, Object> distinctObjects = new HashMap<String, Object>();
        Map<String, Object> source = new HashMap<String, Object>();
        do {
            for (SearchHit hit : scrollResponse.getHits().getHits()) {
                source = hit.getSourceAsMap();
                counterDoc++;

                if (source.get("abstract") != null) {
                    if (source.get("abstract").toString().length() >= 293) {
                        counterAbstract++;
                    }
                }


                List<Object> concepts;

                concepts = (List) source.get("concepts");


                if (source.get("concepts") != null && concepts.size() > 0) {
                    counterConcepts++;
                    avgConcept.add(concepts.size());


                }


                List<Object> keywords;

                keywords = (List) source.get("keywords");
                if (source.get("keywords") != null && keywords.size() > 0) {
                    counterKeywords++;
                    avgKeyword.add(keywords.size());
                }

                if (source.get("abstract") != null && (source.get("keywords") != null && keywords.size() > 0 || (source.get("concepts") != null && concepts.size() > 0))) {
                    if (source.get("abstract").toString().length() >= 293) {
                        counterKeywordsConceptsAbstract++;
                    }

                    // avgKeyword.add(keywords.size());
                }


            }
            scrollResponse = client.prepareSearchScroll(scrollResponse.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
        }
        while (scrollResponse.getHits().getHits().length != 0); // Zero hits mark the end of the scroll and the while loop.
        System.out.println("");
        System.out.println(INDEX);

        System.out.println("Docs: " + counterDoc);
        System.out.println("Doc with Abstracts with more than 293 letters: " + counterAbstract);
        System.out.println("Concepts: " + counterConcepts);

        double avgConcepts = 0;
        double avgKeywords = 0;


        Integer[] avgKeywordArr = new Integer[avgConcept.size()];

        double standardDeviationConcepts = calculateSD(avgConcept);
        double standardDeviationKeywords = calculateSD(avgKeyword);


        //avgConcept.forEach(x-> System.out.println(x));


        for (Integer integer : avgConcept) {
            avgConcepts += integer;
        }

        for (Integer integer : avgKeyword) {
            avgKeywords += integer;
        }

        if (avgConcept.size() != 0) {

            System.out.println("Avg Concepts: " + (avgConcepts / avgConcept.size()));
        } else {
            System.out.println("Avg Concepts: " + 0);
        }
        System.out.println("Keywords: " + counterKeywords);
        System.out.println("Docs Complete: " + counterKeywordsConceptsAbstract);

        if (avgKeyword.size() != 0) {
            System.out.println("Avg Keywords: " + (avgKeywords / avgKeyword.size()));
        } else {
            System.out.println("Avg Keywords: " + 0);
        }

        System.out.println("SD Concepts: " + standardDeviationConcepts);
        System.out.println("SD Keywords: " + standardDeviationKeywords);
        client.close();
    }


    public static double calculateSD(List<Integer> numArray) {
        double sum = 0.0, standardDeviation = 0.0;
        int length = numArray.size();

        for (double num : numArray) {
            sum += num;
        }

        double mean = sum / length;

        for (double num : numArray) {
            standardDeviation += Math.pow(num - mean, 2);
        }

        return Math.sqrt(standardDeviation / length);
    }

}
