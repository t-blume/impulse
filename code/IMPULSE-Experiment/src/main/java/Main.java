import analyzer.DatasetStatistics;
import analyzer.Deduplicate;
import connector.ElasticsearchClient;
import helper.DataItem;

import java.io.IOException;


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

    private static final String TYPE = "publication";
    private static final int BULK_SIZE = 5000;

    /*
        TODO FIXME: index data and analyze data in same run does not work!
     */
    private static void indexData(String index, String filename) {
        ElasticsearchClient elasticsearchClient = new ElasticsearchClient(index, TYPE, BULK_SIZE);
        try {
            if(elasticsearchClient.exists())
                System.out.println("Deleted previous index: " + elasticsearchClient.clear());
            int[] result = elasticsearchClient.bulkUploadFile(filename);
            System.out.println("Uploaded: " + result[0]);
            System.out.println("Failed: " + result[1]);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                elasticsearchClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }





    private static void deduplicate(String index, DataItem.InputType inputType) {
        ElasticsearchClient elasticsearchClient = new ElasticsearchClient(index, TYPE, BULK_SIZE);
        Deduplicate deduplicate = new Deduplicate(elasticsearchClient, inputType);
        try {
            deduplicate.findAllDuplicates();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                elasticsearchClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    private static void analyzeDataset(String index, DataItem.InputType inputType) {
        ElasticsearchClient elasticsearchClient = new ElasticsearchClient(index, TYPE, BULK_SIZE);
        DatasetStatistics datasetStatistics = new DatasetStatistics(elasticsearchClient, inputType);
        try {
            datasetStatistics.runStatistics();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                elasticsearchClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        //TODO add parameter
        boolean reIndexData = false;

        String btc14Index = "btc-14";
        String btc14File = "testresources/context-sample-rdf-data.json";

        String zbwIndex = "zbw";
        String zbwFile = "testresources/zbw-res_2019-09-04.json";

        if (reIndexData) {
            indexData(btc14Index, btc14File);
            indexData(zbwIndex, zbwFile);
        }
        else {
//            deduplicate(btc14Index, DataItem.InputType.MOVING);
//            analyzeDataset(btc14Index, DataItem.InputType.MOVING);


            deduplicate(zbwIndex, DataItem.InputType.ZBW);
            analyzeDataset(zbwIndex, DataItem.InputType.ZBW);
        }


    }

}



