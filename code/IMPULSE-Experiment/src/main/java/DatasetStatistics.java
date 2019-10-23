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
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

/**
 * Get Information about a single generated dataset.
 */
public class DatasetStatistics {
    private String INDEX;
    private String TYPE;

    private int counterAbstract = 0;
    private int counterDoc = 0;
    private int counterConcepts = 0;
    private int counterKeywords = 0;
    private int counterKeywordsConceptsAbstract = 0;

    private List<Integer> avgConcept = new ArrayList<>();
    private List<Integer> avgKeyword = new ArrayList<>();


    public DatasetStatistics(String index, String type) {
        this.INDEX = index;
        this.TYPE = type;
    }

    public void analyze() throws UnknownHostException {
        Client client = new PreBuiltTransportClient(
                Settings.builder().put("client.transport.sniff", true)
                        .put("cluster.name", "elasticsearch").build())
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));


        //scroll through the whole index
        SearchResponse scrollResponse = client.prepareSearch(INDEX).setTypes(TYPE)
                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(new TimeValue(60000))
                .setQuery(matchAllQuery())
                .setSize(100)
                .get();

        while (scrollResponse.getHits().getHits().length != 0) {
            for (SearchHit hit : scrollResponse.getHits().getHits()) {
                Map<String, Object> source = hit.getSourceAsMap();

                boolean hasAbstract = false;
                boolean hasKeywords = false;
                boolean hasConcepts = false;

                if (source.get("abstract") != null)
                    if (source.get("abstract").toString().length() >= 10)
                        hasAbstract = true;


                List<Object> concepts = (List) source.get("concepts");
                if (source.get("concepts") != null && concepts.size() > 0) {
                    hasConcepts = true;
                    avgConcept.add(concepts.size());
                }

                List<Object> keywords = (List) source.get("keywords");
                if (source.get("keywords") != null && keywords.size() > 0) {
                    hasKeywords = true;
                    avgKeyword.add(keywords.size());
                }
                //increment respective counters
                counterDoc++;
                if (hasAbstract)
                    counterAbstract++;
                if (hasConcepts)
                    counterConcepts++;
                if (hasKeywords)
                    counterKeywords++;
                if (hasAbstract && hasConcepts && hasKeywords)
                    counterKeywordsConceptsAbstract++;

                //get next set of items
                scrollResponse = client.prepareSearchScroll(scrollResponse.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
            }
        }
        client.close();
        System.out.println("_______________________________________");
        System.out.println(INDEX);
        System.out.println("Total Docs: " + counterDoc);
        System.out.println("Docs with Abstracts" + counterAbstract);
        System.out.println("Docs with Concepts: " + counterConcepts);
        System.out.println("Docs with Keywords: " + counterKeywords);
        System.out.println("Docs complete: " + counterKeywordsConceptsAbstract);
        System.out.println("---------------------------------------");

        double avgConcepts = 0;
        double avgKeywords = 0;


        double standardDeviationConcepts = calculateSD(avgConcept);
        double standardDeviationKeywords = calculateSD(avgKeyword);


        //sum up all concepts
        for (Integer integer : avgConcept)
            avgConcepts += integer;

        //sum up all keywords
        for (Integer integer : avgKeyword)
            avgKeywords += integer;


        if (avgConcept.size() != 0)
            System.out.println("Avg. Concepts/Doc: " + (avgConcepts / avgConcept.size()) + " (" + standardDeviationConcepts + ")");
         else
            System.out.println("Avg. Concepts/Doc: 0 (0)");


        if (avgKeyword.size() != 0)
            System.out.println("Avg. Keywords/Doc: " + (avgKeywords / avgKeyword.size())+ " (" + standardDeviationKeywords + ")");
         else
            System.out.println("Avg. Keywords/Doc: 0 (0)");

        System.out.println("_______________________________________");
    }


    public static double calculateSD(List<Integer> numArray) {
        double sum = 0.0, standardDeviation = 0.0;
        int length = numArray.size();

        for (double num : numArray)
            sum += num;


        double mean = sum / length;

        for (double num : numArray)
            standardDeviation += Math.pow(num - mean, 2);

        return Math.sqrt(standardDeviation / length);
    }
}
