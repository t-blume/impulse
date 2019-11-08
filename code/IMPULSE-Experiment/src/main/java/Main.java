import analyzer.DatasetStatistics;
import analyzer.Deduplicate;
import analyzer.RecordLinkage;
import connector.ElasticsearchClient;
import helper.DataItem;
import helper.LinkStats;
import helper.Utils;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;


/**
 *
 */
public class Main {
    private static final Logger logger = LogManager.getLogger(Main.class.getSimpleName());

    private static final String TYPE = "_doc";
    private static final int BULK_SIZE = 5000;


    public static void main(String[] args) {
        Options options = new Options();

        ////////////////////////////////////////////
        OptionGroup actionGroup = new OptionGroup();
        Option indexOption = new Option("i", "index", true, "Upload file to the ES index.");
        indexOption.setArgs(2);
        indexOption.setArgName("<index> <file>");
        actionGroup.addOption(indexOption);

        Option deduplicateOption = new Option("d", "deduplicate", true, "Deduplicate the ES index.");
        deduplicateOption.setArgs(2);
        deduplicateOption.setArgName("<index> <type>");
        actionGroup.addOption(deduplicateOption);

        Option analyzeOption = new Option("a", "analyze", true, "Analyze the ES index.");
        analyzeOption.setArgs(2);
        analyzeOption.setArgName("<index> <type>");
        actionGroup.addOption(analyzeOption);

        Option linkOption = new Option("l", "link", true, "Link records of two ES indices.");
        linkOption.setArgs(4);
        linkOption.setArgName("<index1> <type1> <index2> <type2>");
        actionGroup.addOption(linkOption);

        actionGroup.setRequired(true);
        options.addOptionGroup(actionGroup);
        //////////////////////////////////
        Option outputFolderOption = new Option("o", "out", true, "Folder where to store output.");
        outputFolderOption.setArgName("folder");
        outputFolderOption.setRequired(true);
        options.addOption(outputFolderOption);
        //////////////////////////////////
        Options optionsHelp = new Options();
        Option help = new Option("h", "help", false, "print help");
        optionsHelp.addOption(help);
        //////////////////////////////////
        CommandLineParser parserHelp = new DefaultParser();
        HelpFormatter formatterHelp = new HelpFormatter();
        CommandLine cmdHelp;

        // this parses the command line just for help and doesn't throw an exception on unknown options
        try {
            // parse the command line arguments for the help option
            cmdHelp = parserHelp.parse(optionsHelp, args, true);
            // print help
            if (cmdHelp.hasOption("h") || cmdHelp.hasOption("help")) {
                formatterHelp.printHelp(80, " ", "CDMHarvester\n",
                        options, "\n", true);
                System.exit(0);
            }

        } catch (ParseException e1) {
            formatterHelp.printHelp(80, " ", "ERROR: " + e1.getMessage() + "\n",
                    optionsHelp, "\nError occurred! Please see the error message above", true);
            System.exit(-1);
        }

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        // this parses the command line for all other options
        try {
            // parse the command line arguments with the defined options
            cmd = parser.parse(options, args, true);
            // everything's fine, run the program
            run(cmd);
        } catch (ParseException e2) {
            formatter.printHelp(80, " ", "ERROR: " + e2.getMessage() + "\n",
                    options, "\nError occurred! Please see the error message above", true);
            System.exit(-1);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void run(CommandLine cmd) throws IOException {
        String[] innerArgs;
        String outFolder = cmd.getOptionValue("o");
        if (!outFolder.endsWith(File.separator))
            outFolder += File.separator;
        File outFile;


        if (cmd.hasOption("i")) {
            //upload data
            innerArgs = cmd.getOptionValues("i");
            String index = innerArgs[0];
            String fileName = innerArgs[1];
            outFile = Utils.createFile(outFolder + "upload_" + index + ".txt");
            indexData(index, fileName, outFile);
        } else if (cmd.hasOption("d")) {
            //deduplicate index
            innerArgs = cmd.getOptionValues("d");
            String index = innerArgs[0];
            String type = innerArgs[1];
            outFile = Utils.createFile(outFolder + "deduplicate_" + index + ".txt");
            int iterations = 1;
            while (outFile.exists()) {
                outFile = Utils.createFile(outFolder + "deduplicate-" + iterations + "_" + index + ".txt");
                iterations++;
            }
            if (type.equals(DataItem.InputType.MOVING.toString()))
                deduplicate(index, DataItem.InputType.MOVING, outFile);
            else if (type.equals(DataItem.InputType.ZBW.toString()))
                deduplicate(index, DataItem.InputType.ZBW, outFile);
            else
                logger.error("Invalid data item type provided! Supported types are: < MOVING | ZBW >");
        } else if (cmd.hasOption("a")) {
            //analyze the index
            innerArgs = cmd.getOptionValues("a");
            String index = innerArgs[0];
            String type = innerArgs[1];
            outFile = Utils.createFile(outFolder + "analysis_" + index + ".txt");

            if (type.equals(DataItem.InputType.MOVING.toString()))
                analyzeDataset(index, DataItem.InputType.MOVING, outFile);
            else if (type.equals(DataItem.InputType.ZBW.toString()))
                analyzeDataset(index, DataItem.InputType.ZBW, outFile);
            else
                logger.error("Invalid data item type provided! Supported types are: < MOVING | ZBW >");
        } else if (cmd.hasOption("l")) {
            //link records from indices
            innerArgs = cmd.getOptionValues("l");
            String index1 = innerArgs[0];
            String type1 = innerArgs[1];
            DataItem.InputType inputType1;

            if (type1.equals(DataItem.InputType.MOVING.toString()))
                inputType1 = DataItem.InputType.MOVING;
            else if (type1.equals(DataItem.InputType.ZBW.toString()))
                inputType1 = DataItem.InputType.ZBW;
            else {
                logger.error("Invalid data item type provided! Supported types are: < MOVING | ZBW >");
                return;
            }

            String index2 = innerArgs[2];
            String type2 = innerArgs[3];
            DataItem.InputType inputType2;

            if (type2.equals(DataItem.InputType.MOVING.toString()))
                inputType2 = DataItem.InputType.MOVING;
            else if (type2.equals(DataItem.InputType.ZBW.toString()))
                inputType2 = DataItem.InputType.ZBW;
            else {
                logger.error("Invalid data item type provided! Supported types are: < MOVING | ZBW >");
                return;
            }

            outFile = Utils.createFile(outFolder + "recordLinkage_" + index1 + "-" + index2 + ".txt");
            linkEntities(index1, inputType1, index2, inputType2, outFile);
        }
    }


    private static void indexData(String index, String filename, File outFile) throws IOException {
        logger.info("Indexing data! Uploading \"" + filename + "\" to \"" + index + "\".");
        ElasticsearchClient elasticsearchClient = new ElasticsearchClient(index, TYPE, BULK_SIZE);
        BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));
        try {
            if (elasticsearchClient.exists())
                logger.info("Deleted previous index: " + elasticsearchClient.clear());

            int[] result = elasticsearchClient.bulkUploadFile(filename);
            writer.write("Uploaded: " + result[0]);
            writer.newLine();
            writer.write("Failed: " + result[1]);
            writer.newLine();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                elasticsearchClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        logger.info("Uploading done. Find output in \"" + outFile + "\".");
    }


    private static void deduplicate(String index, DataItem.InputType inputType, File outFile) throws IOException {
        logger.info("Deduplication of \"" + index + "\"");

        ElasticsearchClient elasticsearchClient = new ElasticsearchClient(index, TYPE, BULK_SIZE);
        BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));

        Deduplicate deduplicate = new Deduplicate(elasticsearchClient, inputType);
        try {
            int[] stats = deduplicate.findAllDuplicates();
            writer.write("Number of unique titles in index: " + stats[0]);
            writer.newLine();
            writer.write("Deleted documents: " + stats[1]);
            writer.newLine();
            writer.write("Merged documents: " + stats[2]);
            writer.newLine();
            writer.close();
            logger.info("Deduplication done. Find output in \"" + outFile + "\".");
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


    private static void analyzeDataset(String index, DataItem.InputType inputType, File outFile) throws IOException {
        logger.info("Analysis of \"" + index + "\"");
        ElasticsearchClient elasticsearchClient = new ElasticsearchClient(index, TYPE, BULK_SIZE);
        BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));
        DatasetStatistics datasetStatistics = new DatasetStatistics(elasticsearchClient, inputType);
        try {
            datasetStatistics.runStatistics(writer);
            logger.info("Analysis done. Find output in \"" + outFile + "\".");
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


    private static void linkEntities(String indexOne, DataItem.InputType inputTypeOne,
                                     String indexTwo, DataItem.InputType inputTypeTwo,
                                     File outFile) throws IOException {
        logger.info("Record Linkage between \"" + indexOne + "\" and \"" + indexTwo + "\".");
        ElasticsearchClient elasticsearchClientOne = new ElasticsearchClient(indexOne, TYPE, BULK_SIZE);
        ElasticsearchClient elasticsearchClientTwo = new ElasticsearchClient(indexTwo, TYPE, BULK_SIZE);
        BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));

        RecordLinkage recordLinkage = new RecordLinkage(elasticsearchClientOne, inputTypeOne,
                elasticsearchClientTwo, inputTypeTwo);
        try {
            List<LinkStats> linkedStats = recordLinkage.linkDatasets();
            int newAbstracts = 0;
            int totalAddedConcepts = 0;
            int totalAddedKeywords = 0;
            List<Integer> addedKeywords = new LinkedList<>();
            List<Integer> addedConcepts = new LinkedList<>();
            int nulls = 0;


            for (LinkStats linkStats : linkedStats) {
                if (linkStats == null)
                    nulls++;
                else {
                    if (linkStats._newAbstract)
                        newAbstracts++;
                    if(linkStats._newConcepts > 0) {
                        totalAddedConcepts += linkStats._newConcepts;
                        addedConcepts.add(linkStats._newConcepts);
                    }
                    if(linkStats._newKeywords > 0) {
                        totalAddedKeywords += linkStats._newKeywords;
                        addedKeywords.add(linkStats._newKeywords);
                    }
                }
            }
            writer.write("Docs with added abstract," + newAbstracts);
            writer.newLine();
            writer.write("Docs with added keywords," + addedKeywords.size());
            writer.newLine();
            writer.write("Docs with added concept," + addedConcepts.size());
            writer.newLine();
            writer.write("Total added keywords," + totalAddedKeywords);
            writer.newLine();
            writer.write("Avg added keywords," + ((double)totalAddedKeywords / (double) addedKeywords.size()));
            writer.newLine();
            writer.write("SD added keywords," + Utils.calculateSD(addedKeywords));
            writer.newLine();
            writer.write("Total added concepts," + totalAddedConcepts);
            writer.newLine();
            writer.write("Avg added concepts," + ((double)totalAddedConcepts / (double) addedConcepts.size()));
            writer.newLine();
            writer.write("SD added concepts," + Utils.calculateSD(addedConcepts));
            writer.newLine();
            writer.write("Errors: " + nulls);
            writer.newLine();
            writer.close();
            logger.info("Record Linkage done. Find output in \"" + outFile + "\".");

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                elasticsearchClientOne.close();
                elasticsearchClientTwo.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}



