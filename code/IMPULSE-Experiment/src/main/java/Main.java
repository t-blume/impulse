import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;


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
            System.out.println("Deleted previous index: " + elasticsearchClient.clear());
            int[] result = elasticsearchClient.bulkUploadFile(filename);
            System.out.println("Uploaded: " + result[0]);
            System.out.println("Failed: " + result[1]);
        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            try {
                elasticsearchClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    private static void analyzeDataset(String index){
        ElasticsearchClient elasticsearchClient = new ElasticsearchClient(index, TYPE, BULK_SIZE);
        DatasetStatistics datasetStatistics = new DatasetStatistics(elasticsearchClient);
        try {
            datasetStatistics.runStatistics();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
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

        String index = "impulse-test";
        String filename = "testresources/context-sample-rdf-data.json";

        if(reIndexData)
            indexData(index, filename);

        analyzeDataset(index);


}

}



