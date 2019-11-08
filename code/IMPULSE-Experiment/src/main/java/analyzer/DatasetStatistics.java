package analyzer;

import connector.ElasticsearchClient;
import helper.DataItem;
import helper.Utils;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

import static helper.Utils.*;

/**
 * Get Information about a single generated dataset.
 */
public class DatasetStatistics {

    private static final int TOP_K_PLDs = 10;

    private ElasticsearchClient client;
    private DataItem.InputType inputType;

    private int counterAbstract = 0;
    private int counterDoc = 0;
    private int counterConcepts = 0;
    private int counterKeywords = 0;
    private int counterKeywordsConceptsAbstract = 0;

    private List<Integer> avgConcept = new ArrayList<>();
    private List<Integer> avgKeyword = new ArrayList<>();

    private Set<String> contextURIs = new HashSet<>();
    private Map<String, Integer> documentCountPerPayLevelDomain = new HashMap<>();

    public DatasetStatistics(ElasticsearchClient client, DataItem.InputType inputType) {
        this.client = client;
        this.inputType = inputType;
    }

    public void runStatistics(BufferedWriter writer) throws IOException {
        //scroll through the whole index
        SearchHits hits = client.search(null, 100);
        while (hits.getHits().length != 0) {
            for (SearchHit hit : hits.getHits()) {
                DataItem dataItem = new DataItem();
                if (inputType == DataItem.InputType.MOVING)
                    dataItem.parseMOVING(hit.getSourceAsMap(), hit.getId());
                else if(inputType == DataItem.InputType.ZBW)
                    dataItem.parseZBW(hit.getSourceAsMap(), hit.getId());

                boolean hasAbstract = false;
                boolean hasKeywords = false;
                boolean hasConcepts = false;

                if (dataItem._abstract != null)
                    if (dataItem._abstract.length() >= 10)
                        hasAbstract = true;


                if (dataItem._concepts != null && dataItem._concepts.size() > 0) {
                    hasConcepts = true;
                    avgConcept.add(dataItem._concepts.size());
                }

                if (dataItem._keywords != null && dataItem._keywords.size() > 0) {
                    hasKeywords = true;
                    avgKeyword.add(dataItem._keywords.size());
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


                if (dataItem._sourceURLs != null) {
                    Set<String> payLevelDomains = new HashSet<>();
                    for (String sourceURI : dataItem._sourceURLs) {
                        contextURIs.add(sourceURI);
                        try {
                            String pld = extractPLD(normalizeURL(sourceURI));
                            if (pld == null)
                                System.out.println(hit);
                            payLevelDomains.add(pld);
                        } catch (URISyntaxException e) {
                            e.printStackTrace();
                        }
                    }

                    for (String pld : payLevelDomains)
                        documentCountPerPayLevelDomain.merge(pld, 1, (O, N) -> O + N);
                }
            }
            //get next set of items
            hits = client.scroll();
        }

        writer.write("Total Docs," + counterDoc);
        writer.newLine();
        writer.write("Docs with Abstracts," + counterAbstract);
        writer.newLine();
        writer.write("Docs with Concepts," + counterConcepts);
        writer.newLine();
        writer.write("Docs with Keywords," + counterKeywords);
        writer.newLine();
        writer.write("Docs complete," + counterKeywordsConceptsAbstract);
        writer.newLine();

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


        if (avgConcept.size() != 0) {
            writer.write("Avg. Concepts/Doc," + (avgConcepts / avgConcept.size()));
            writer.newLine();
            writer.write("SD Concepts/Doc," + standardDeviationConcepts);
            writer.newLine();
        }
        else{
            writer.write("Avg. Concepts/Doc,0");
            writer.newLine();
            writer.write("SD Concepts/Doc,0");
            writer.newLine();
        }

        if (avgKeyword.size() != 0) {
            writer.write("Avg. Keywords/Doc," + (avgKeywords / avgKeyword.size()));
            writer.newLine();
            writer.write("SD Keywords/Doc," + standardDeviationKeywords);
            writer.newLine();
        }
        else{
            writer.write("Avg. Keywords/Doc,0");
            writer.newLine();
            writer.write("SD Keywords/Doc,0");
            writer.newLine();
        }

        writer.write("Number of PLDs," + documentCountPerPayLevelDomain.keySet().size());
        writer.newLine();

        Map<String, Integer> sortedPLDs = Utils.sortByValue(documentCountPerPayLevelDomain);
        int c = 0;
        Iterator<Map.Entry<String, Integer>> iterator = sortedPLDs.entrySet().iterator();
        while (c < TOP_K_PLDs && iterator.hasNext()) {
            Map.Entry<String, Integer> pld = iterator.next();
            writer.write(pld.getKey() + "," + pld.getValue());
            writer.newLine();
            writer.write(pld.getKey() +"," + Math.round((double) pld.getValue() / (double) counterDoc * 100) + "%");
            writer.newLine();
            c++;
        }
        writer.close();
    }




}
