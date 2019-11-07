package analyzer;

import connector.ElasticsearchClient;
import helper.DataItem;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.util.*;

import static helper.Utils.*;

public class Deduplicate {

    private ElasticsearchClient client;
    private DataItem.InputType inputType;

    public Deduplicate(ElasticsearchClient client, DataItem.InputType inputType) {
        this.client = client;
        this.inputType = inputType;
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
            //System.out.println("title: " + title);
            hits = client.search(title, 100);
            List<DataItem> documentSubset = new LinkedList<>();
            while (hits.getHits().length != 0) {
                for (SearchHit hit : hits.getHits()) {
                    DataItem dataItem = new DataItem();
                    if(inputType == DataItem.InputType.MOVING)
                        dataItem.parseMOVING(hit.getSourceAsMap(), hit.getId());
                    else if(inputType == DataItem.InputType.ZBW)
                        dataItem.parseZBW(hit.getSourceAsMap(), hit.getId());
                    documentSubset.add(dataItem);
                }
                //get next set of items
                hits = client.scroll();
            }
            handleDuplicates(documentSubset);
            client.releaseScrollContext();
        }
    }


    private void handleDuplicates(List<DataItem> documentList) {
        Set<DataItem> duplicates = new HashSet<>();
        Map<String, DataItem> mergedItems = new HashMap<>();
        for (int i = 0; i < documentList.size(); i++) {
            DataItem dataItem1 = documentList.get(i);
            //normalize title1 string
            String title1 = normalizeStrings(dataItem1._title);
            //get all other candidates
            for (int j = 1; j < documentList.size() - 1; j++) {
                DataItem dataItem2 = documentList.get(j);
                if (dataItem1._id == dataItem2._id)
                    continue;
                //normalize title2 string
                String title2 = normalizeStrings(dataItem2._title);
                //compare
                if (compareTitles(title1, title2)) {
                    //they check out
                    int sameAuthors = 0;
                    for (DataItem.Person a1 : dataItem1._authorList) {
                        String author1 = normalizeStrings(a1._rawName);
                        for (DataItem.Person a2 : dataItem2._authorList) {
                            String author2 = normalizeStrings(a2._rawName);
                            if (compareAuthors(author1, author2))
                                sameAuthors++;
                        }
                    }
                    if (sameAuthors > 0) {
                        //share at least one author

                        //the other document was already merged
                        if (mergedItems.containsKey(dataItem2._id)) {
                            dataItem1.merge(mergedItems.get(dataItem2._id));
                            mergedItems.remove(dataItem2._id);
                        } else
                            dataItem1.merge(dataItem2);

                        mergedItems.put(dataItem1._id, dataItem1);
                        duplicates.remove(dataItem1);
                        duplicates.add(dataItem2);
                    }
                }
            }
        }
        if (duplicates.size() > 0 || mergedItems.size() > 0) {
            System.out.println("New iteration:");
            duplicates.forEach(item -> {
                try {
                    System.out.println("deleting " + item);
                    client.delete(item._id);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            mergedItems.forEach((key, item) -> {
                try {
                    System.out.println("Updating " + item);
                    if(inputType == DataItem.InputType.MOVING)
                        client.update(item._id, item.reverseParseMOVING());
                    else if(inputType == DataItem.InputType.ZBW)
                        client.update(item._id, item.reverseParseZBW());

                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            System.out.println("____________________________________");
        }

    }


}
