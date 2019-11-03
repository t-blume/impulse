import org.apache.logging.log4j.util.SortedArrayStringMap;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Get Information about a single generated dataset.
 */
public class DatasetStatistics {

    private static final int TOP_K_PLDs = 10;

    private ElasticsearchClient client;

    private int counterAbstract = 0;
    private int counterDoc = 0;
    private int counterConcepts = 0;
    private int counterKeywords = 0;
    private int counterKeywordsConceptsAbstract = 0;

    private List<Integer> avgConcept = new ArrayList<>();
    private List<Integer> avgKeyword = new ArrayList<>();

    private Set<String> contextURIs = new HashSet<>();
    private Map<String, Integer> documentCountPerPayLevelDomain = new HashMap<>();

    public DatasetStatistics(ElasticsearchClient client) {
        this.client = client;
    }

    public void runStatistics() throws IOException {
        //scroll through the whole index
        SearchHits hits = client.search(null, 100);

        while (hits.getHits().length != 0) {
            for (SearchHit hit : hits.getHits()) {
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


                if (source.get("sourceURLs") != null) {
                    List<String> sourceURIs = (List) source.get("sourceURLs");
                    Set<String> payLevelDomains = new HashSet<>();
                    for (String sourceURI : sourceURIs) {
                        contextURIs.add(sourceURI);
                        try {
                            payLevelDomains.add(extractPLD(sourceURI));
                        } catch (URISyntaxException e) {
                            e.printStackTrace();
                        }
                    }

                    for (String pld : payLevelDomains)
                        documentCountPerPayLevelDomain.merge(pld, 1, (O,N) -> O + N);
                }
            }
            //get next set of items
            hits = client.scroll();
        }
        System.out.println("_______________________________________");
        System.out.println(client.getIndex());
        System.out.println("Total Docs: " + counterDoc);
        System.out.println("Docs with Abstracts: " + counterAbstract);
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
            System.out.println("Avg. Keywords/Doc: " + (avgKeywords / avgKeyword.size()) + " (" + standardDeviationKeywords + ")");
        else
            System.out.println("Avg. Keywords/Doc: 0 (0)");

        System.out.println("_______________________________________");
        System.out.println("Number of PLDs: " + documentCountPerPayLevelDomain.keySet().size());


        Map<String, Integer> sortedPLDs = Utils.sortByValue(documentCountPerPayLevelDomain);
        int c = 0;
        Iterator<Map.Entry<String, Integer>> iterator = sortedPLDs.entrySet().iterator();
        while (c < TOP_K_PLDs && iterator.hasNext()){
            Map.Entry<String, Integer> pld = iterator.next();
            System.out.println(pld.getKey() + ": " + pld.getValue() + " (" + Math.round((double) pld.getValue()/ (double) counterDoc * 100) + "%)");
            c++;
        }
    }


    //----------------------------------------------------//
    // ----- HELPER

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


    /**
     * proper way to extract PLD
     * @param uri
     * @return
     * @throws URISyntaxException
     */
    public static String extractPLD(String uri) throws URISyntaxException {
        if (uri == null)
            return null;
        if(uri.trim().isEmpty())
            return "";

        URI parsedUri = new URI(uri);

        String pld = parsedUri.getHost();

        return pld;
    }
}
