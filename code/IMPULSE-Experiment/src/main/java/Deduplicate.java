import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.util.*;

public class Deduplicate {

    private ElasticsearchClient client;

    public Deduplicate(ElasticsearchClient client) {
        this.client = client;
    }

    /**
     * Get all titles in dataset.
     * Query all items with the same title.
     * Check all items with same title for possible duplicate.
     *
     * @throws IOException
     */
    public void findAllDuplicates() throws IOException {
        Set<String> allTitles = new HashSet<>();
        SearchHits hits = client.search(null, 100);
        while (hits.getHits().length != 0) {
            for (SearchHit hit : hits.getHits()) {
                Map<String, Object> source = hit.getSourceAsMap();
                if (source.get("title") != null)
                    allTitles.add((String) source.get("title"));
            }
            //get next set of items
            hits = client.scroll();
        }

        System.out.println(allTitles.size());
        for (String title : allTitles) {
            hits = client.search(title, 100);
//            System.out.println(">>>>>>>>>>>>>>>>------------------");
            Collection<Map<String, Object>> documentSubset = new HashSet<>();
            while (hits.getHits().length != 0) {
                for (SearchHit hit : hits.getHits()) {
                    documentSubset.add(hit.getSourceAsMap());
//                    debugDocument(hit.getSourceAsMap());
                }

                //get next set of items
                hits = client.scroll();
            }
//            System.out.println("<<<<<<<<<<<<<<<<---------------------");
            mergeToMinimalSet(documentSubset);
        }
    }


    private void mergeToMinimalSet(Collection<Map<String, Object>> documentCollection) {
        Map<String, Set<Map<String, Object>>> authorBuckets = new HashMap<>();
        //all documents in documentCollection share the same title
        //index all authors of each document to find documents that share at least k authors
        for (Map<String, Object> documentSource : documentCollection) {
            Set<Map<String, Object>> thisDocumentSet = new HashSet<>();
            thisDocumentSet.add(documentSource);
            List<Object> authorList = (List) documentSource.get("metadata_persons");
            for (int i = 0; i < authorList.size(); i++) {
                HashMap<String, String> author = (HashMap) authorList.get(i);
                if (author.get("rawName") != null) {
                    //TODO FIXME Proper comparison
                    String authorName = author.get("rawName").toLowerCase().trim().replaceAll("\\.|\\,|\\:|\\;", "");
                    authorBuckets.merge(authorName, thisDocumentSet, (o, n) -> {
                        o.addAll(n);
                        return o;
                    });
                }
            }
        }
        //each bucket in authorBuckets contains the documents the author has written.
        //If bucket contains more than one document, merge these documents and repeat from start
        for (Set<Map<String, Object>> documentSets : authorBuckets.values()) {
            if (documentSets.size() > 1) {
                System.out.println("-----found duplicates -------");
                documentSets.forEach(D -> debugDocument(D));
                System.out.println("_______________________________");
            }
        }
    }


    private void debugDocument(Map<String, Object> documentSource) {
        List<Object> authorList = (List) documentSource.get("metadata_persons");
        for (int i = 0; i < authorList.size(); i++) {
            HashMap<String, String> author = (HashMap) authorList.get(i);
            if (author.get("rawName") != null) {
                System.out.print(author.get("rawName") + ",");
            }
        }
        System.out.print(": " + documentSource.get("title") + "\n");

    }

}
