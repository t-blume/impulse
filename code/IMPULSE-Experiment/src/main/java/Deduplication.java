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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

public class Deduplication {
    private String INDEX;
    private String TYPE;
    private Boolean fuzzy;
    //   private static HashMap<String, Object> dedup = new HashMap<String, Object>();
    private static HashMap<String, Object> dedup = new HashMap<String, Object>();
    private static List<String> docs2Delete = new ArrayList<String>();
    private static List<String> docs2Update = new ArrayList<String>();
    private int counter = 0;


    public Deduplication(String index, String type, Boolean fuzzy) {
        this.INDEX = index;
        this.TYPE = type;
        this.fuzzy = fuzzy;
    }

    public void deduplication() throws UnknownHostException {
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


        int counter = 0;
        // Map<String, Object> distinctObjects = new HashMap<String, Object>();
        Map<String, Object> docFromIndex = new HashMap<String, Object>();
        do {
            for (SearchHit hit : scrollResponse.getHits().getHits()) {
                counter++;
//if (counter >=2000000) {
    docFromIndex = hit.getSourceAsMap();

    docFromIndex.put("doc_id", hit.getId());


    if (docFromIndex.get("title") != null && docFromIndex.get("metadata_persons") != null) {
        String s = docFromIndex.get("title").toString();

        if (fuzzy) {
            if (fuzzyMatchingMap(s)) {
                //  System.out.println("Fuzzy matching: duplicate doc: " + s);
                doDeduplication(docFromIndex, s, hit);
            } else {
                dedup.put(s, docFromIndex);
            }
        } else {
            if (dedup.containsKey(s)) {
                //  System.out.println("Exact matching: duplicate doc: " + s);
                doDeduplication(docFromIndex, s, hit);
            } else {
                dedup.put(s, docFromIndex);
            }
        }

    } else {
        counter++;
        docs2Delete.add(hit.getId());
    }
}
//            }
            scrollResponse = client.prepareSearchScroll(scrollResponse.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
        }
        while (scrollResponse.getHits().getHits().length != 0&& counter <=5000000); // Zero hits mark the end of the scroll and the while loop.
//        while (scrollResponse.getHits().getHits().length != 0); // Zero hits mark the end of the scroll and the while loop.


        System.out.println("Duplicate documents found: " + docs2Delete.size());


        updateIndex();
//        System.out.println("Dedups: " + dedup);
//        System.out.println("Zu Löschen: " + docs2Delete);


        //   System.out.println("Abstracts with more than 293 letters: " + counter);


        client.close();
    }

    private void doDeduplication(Map<String, Object> docFromIndex, String s, SearchHit hit) {

        List<Object> authorList;

        authorList = (List) docFromIndex.get("metadata_persons");
        for (int i = 0; i < authorList.size(); i++) {

            HashMap<String, String> author = (HashMap) authorList.get(i);
            //System.out.println("author: " + author);
            if (dedup.get(s).toString().contains(author.get("rawName"))) {
                //  System.out.println("Dedup contains author!");
                Map<String, Object> docAlreadyInMap = (Map<String, Object>) dedup.get(s);

                //Check if the Document from the Index has a longer Abstract
                if (docAlreadyInMap.get("abstract") != null && docFromIndex.get("abstract") != null && docAlreadyInMap.get("abstract").toString().length() <= docFromIndex.get("abstract").toString().length()) {
                    mergeDocs(docAlreadyInMap, docFromIndex.get("abstract").toString(), "abstract");
                }

                //Conceptcheck and Keywordcheck:
                //Check if concepts from are not in the conceptlist of the document in the local map
                List conceptsDocAlreadyInMap = (List) docAlreadyInMap.get("concepts");
                List conceptsDocFromIndex = (List) docFromIndex.get("concepts");

                List keywordsDocAlreadyInMap = (ArrayList) docAlreadyInMap.get("keywords");
                List keywordsDocFromIndex = (ArrayList) docFromIndex.get("keywords");

                if (conceptsDocAlreadyInMap != null && conceptsDocFromIndex != null) {
                    conceptsDocFromIndex.forEach(x -> {
                        if (!conceptsDocAlreadyInMap.contains(x)) {
                            mergeDocs(docAlreadyInMap, x, "concept");

                        }
                    });
                }

                if (keywordsDocAlreadyInMap != null && keywordsDocFromIndex != null) {
                    keywordsDocFromIndex.forEach(x -> {
                        if (!keywordsDocAlreadyInMap.contains(x)) {
                            mergeDocs(docAlreadyInMap, x, "keyword");
                        }
                    });
                }

                docs2Delete.add(hit.getId());
                dedup.put(s, docAlreadyInMap);
                break;
            }
        }


    }

    private boolean fuzzyMatchingMap(String s) {
        s = s.toLowerCase();
        Boolean match = false;
        for (Map.Entry<String, Object> entry : dedup.entrySet()) {

            String titleFromMap = entry.getKey().toLowerCase();

            String[] parts1 = s.split(" ");
            String[] parts2 = titleFromMap.split(" ");

            for (int i = 0; i < parts1.length; i++) {
                try {
                    if (parts1[i].length() <= 2) {
                        if (computeLevenshteinDistance(parts1[i], parts2[i]) == 0) {
                            match = true;

                        } else {
                            match = false;
                        }

                    } else if (parts1[i].length() >= 3 && parts1[i].length() <= 5) {
                        if (computeLevenshteinDistance(parts1[i], parts2[i]) <= 1) {
                            match = true;

                        } else {
                            match = false;
                        }

                    } else if (parts1[i].length() > 5) {
                        if (computeLevenshteinDistance(parts1[i], parts2[i]) <= 2) {
                            match = true;

                        } else {
                            match = false;
                        }
                    }
                } catch (ArrayIndexOutOfBoundsException e) {
                    e.printStackTrace();
                }
            }
        }
        return match;
    }


    private static int minimum(int a, int b, int c) {
        return Math.min(Math.min(a, b), c);
    }

    public static int computeLevenshteinDistance(CharSequence lhs, CharSequence rhs) {
        int[][] distance = new int[lhs.length() + 1][rhs.length() + 1];

        for (int i = 0; i <= lhs.length(); i++)
            distance[i][0] = i;
        for (int j = 1; j <= rhs.length(); j++)
            distance[0][j] = j;

        for (int i = 1; i <= lhs.length(); i++)
            for (int j = 1; j <= rhs.length(); j++)
                distance[i][j] = minimum(
                        distance[i - 1][j] + 1,
                        distance[i][j - 1] + 1,
                        distance[i - 1][j - 1] + ((lhs.charAt(i - 1) == rhs.charAt(j - 1)) ? 0 : 1));

        return distance[lhs.length()][rhs.length()];
    }


    private void mergeDocs(Map<String, Object> docAlreadyInMap, Object attribute2Add, String s) {

        if (s.equals("abstract")) {

            docAlreadyInMap.put("abstract", attribute2Add);


        }
        else if (s.equals("concept")) {
            List conceptsDocAlreadyInMap = (List) docAlreadyInMap.get("concepts");
            //  System.out.println("... merging " + s + ": " + attribute2Add +" in Document with Title: " + docAlreadyInMap.get("title"));
            conceptsDocAlreadyInMap.add(attribute2Add);
            docAlreadyInMap.put("concepts", conceptsDocAlreadyInMap);

        } else if (s.equals("keyword")) {

            List keywordsDocAlreadyInMap = (ArrayList) docAlreadyInMap.get("keywords");
            keywordsDocAlreadyInMap.add(attribute2Add);
            docAlreadyInMap.put("keywords", keywordsDocAlreadyInMap);

        }


    }

    private void updateIndex() throws UnknownHostException {
        Client client = new PreBuiltTransportClient(
                Settings.builder().put("client.transport.sniff", true)
                        .put("cluster.name", "elasticsearch").build())
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

        int counter = 0;
        BulkRequestBuilder bulkRequest2 = client.prepareBulk();
//        for (Map.Entry<String, Object> entry : dedup.entrySet()) {
//            counter++;
//
//
//            Map<String, Object> doc2Update = (Map) entry.getValue();
//            String doc_id = (String) doc2Update.get("doc_id");
//            doc2Update.remove("doc_id");
//
//            bulkRequest2.add(client.prepareUpdate()
//                    .setIndex(INDEX)
//                    .setType(TYPE)
//                    .setDoc(doc2Update)
//                    .setId(doc_id));
//
//            if (counter % 10000 == 0) {
//                executeBulk(bulkRequest2);
//            }
//        }
//        executeBulk(bulkRequest2);

//
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        Iterator<String> iter2Delete = docs2Delete.iterator();
            counter=0;
        while (iter2Delete.hasNext()) {
            counter++;

            bulkRequest.add(client.prepareDelete()
                    .setIndex(INDEX)
                    .setType(TYPE)
                    .setId(iter2Delete.next()));
            if (counter % 50000 == 0) {
                executeBulk(bulkRequest);
            }
        }

        executeBulk(bulkRequest);
    }

    private void executeBulk(BulkRequestBuilder bulkRequest2) {



        if ((bulkRequest2 != null) && (!bulkRequest2.request().requests().isEmpty())) {

//            BulkResponse bulkResponse = null;
            BulkResponse bulkResponse = bulkRequest2.execute().actionGet();
            if (bulkResponse.hasFailures()) {
                System.out.println("Elasticsearch updating failed");
            } else {


                System.out.println("Updating  " + INDEX + " finished! ... " + " updated: " + bulkResponse.getItems().length + " documents!");
            }
        } else {
            System.out.println("Elasticsearch updating" + INDEX + "finished without updating any documents");
        }


    }

}
