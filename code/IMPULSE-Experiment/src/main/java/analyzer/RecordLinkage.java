package analyzer;

import connector.ElasticsearchClient;
import helper.DataItem;
import helper.LinkStats;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static helper.Utils.*;

public class RecordLinkage {
    private static final Logger logger = LogManager.getLogger(RecordLinkage.class.getSimpleName());

    private ElasticsearchClient clientOne;
    private DataItem.InputType inputTypeOne;

    private ElasticsearchClient clientTwo;
    private DataItem.InputType inputTypeTwo;

    public RecordLinkage(ElasticsearchClient clientOne, DataItem.InputType inputTypeOne, ElasticsearchClient clientTwo, DataItem.InputType inputTypeTwo) {
        this.clientOne = clientOne;
        this.inputTypeOne = inputTypeOne;
        this.clientTwo = clientTwo;
        this.inputTypeTwo = inputTypeTwo;
    }


    /**
     * Iterate through first dataset and merge into second dataset
     *
     * @throws IOException
     */
    public List<LinkStats> linkDatasets() throws IOException {
        List<LinkStats> linkedStats = new LinkedList<>();
        SearchHits hitsOne = clientOne.search(null, 100);
        while (hitsOne.getHits().length != 0) {
            for (SearchHit hitOne : hitsOne.getHits()) {
                DataItem dataItemOne = new DataItem();
                if (inputTypeOne == DataItem.InputType.MOVING)
                    dataItemOne.parseMOVING(hitOne.getSourceAsMap(), hitOne.getId());
                else if (inputTypeOne == DataItem.InputType.ZBW)
                    dataItemOne.parseZBW(hitOne.getSourceAsMap(), hitOne.getId());
                //access second dataset
                SearchHits hitsTwo = clientTwo.search(dataItemOne._title, 100);
                List<DataItem> candidatesFromTwo = new LinkedList<>();
                while (hitsTwo.getHits().length != 0) {
                    for (SearchHit hitTwo : hitsTwo.getHits()) {
                        DataItem dataItemTwo = new DataItem();
                        if (inputTypeTwo == DataItem.InputType.MOVING)
                            dataItemTwo.parseMOVING(hitTwo.getSourceAsMap(), hitTwo.getId());
                        else if (inputTypeTwo == DataItem.InputType.ZBW)
                            dataItemTwo.parseZBW(hitTwo.getSourceAsMap(), hitTwo.getId());
                        candidatesFromTwo.add(dataItemTwo);
                    }
                    //get next set of items
                    hitsTwo = clientTwo.scroll();
                }

                if (candidatesFromTwo.size() > 0) {
                    logger.debug("Comparing " + candidatesFromTwo.size() + " candidates for title \"" + dataItemOne._title + "\"");
                    LinkStats linkStats = compareWithCandidates(dataItemOne, candidatesFromTwo);
                    if (linkStats != null)
                        linkedStats.add(linkStats);
                }

                clientTwo.releaseScrollContext();
            }
            //get next set of items
            hitsOne = clientOne.scroll();
        }
        clientOne.releaseScrollContext();
        return linkedStats;
    }


    private LinkStats compareWithCandidates(DataItem dataItemOne, List<DataItem> candidatesFromTwo) {
        LinkStats linkStats = null;
        int linkedItems = 0;
        //normalize title1 string
        String title1 = normalizeStrings(dataItemOne._title);
        //get all other candidates
        for (int j = 0; j < candidatesFromTwo.size(); j++) {
            DataItem dataItemTwo = candidatesFromTwo.get(j);
            if (dataItemOne._id == dataItemTwo._id)
                continue;
            //normalize title2 string
            String title2 = normalizeStrings(dataItemTwo._title);
            //compare
            if (compareTitles(title1, title2)) {
                //they check out
                int sameAuthors = 0;
                for (DataItem.Person a1 : dataItemOne._authorList) {
                    String author1 = normalizeStrings(a1._rawName);
                    for (DataItem.Person a2 : dataItemTwo._authorList) {
                        String author2 = normalizeStrings(a2._rawName);
                        if (compareAuthors(author1, author2))
                            sameAuthors++;
                    }
                }
                if (sameAuthors > 0) {
                    //share at least one author
                    linkedItems++;
                    linkStats = new LinkStats();
                    if (linkedItems > 1) {
                        logger.error("Matched document \"" + dataItemOne._id + "\" from index \"" + clientOne.getIndex() + "\" to more than one document in index \"" + clientTwo.getIndex() + "\"!");
                        logger.debug("Linked Item: " + dataItemTwo + " (possibly more to come)");
                    } else {
                        //found a new abstract that was not previously there?
                        if ((dataItemTwo._abstract == null || dataItemTwo._abstract.trim().isEmpty()) && dataItemOne._abstract != null)
                            linkStats._newAbstract = true;

                        if (dataItemOne._keywords != null) {
                            int prev = dataItemTwo._keywords == null ? 0 : dataItemTwo._keywords.size();
                            dataItemTwo._keywords.addAll(dataItemOne._keywords);
                            linkStats._newKeywords = dataItemTwo._keywords.size() - prev;
                        }

                        if (dataItemOne._concepts != null) {
                            int prev = dataItemTwo._concepts == null ? 0 : dataItemTwo._concepts.size();
                            dataItemTwo._concepts.addAll(dataItemOne._concepts);
                            linkStats._newConcepts = dataItemTwo._concepts.size() - prev;
                        }
                    }
                }
            }
        }
        return linkStats;
    }
}
