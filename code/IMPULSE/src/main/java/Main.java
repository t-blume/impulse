package main.java;

import main.java.common.implementation.Mapping;
import main.java.common.interfaces.IInstanceElement;
import main.java.input.implementation.FileQuadSource;
import main.java.input.interfaces.IQuintSource;
import main.java.output.implementation.FileJSONSink;
import main.java.processing.implementation.ContextHarvester;
import main.java.processing.implementation.LODatioQuery;
import main.java.processing.implementation.PLDHarvester;
import main.java.processing.implementation.common.DataItemBuffer;
import main.java.processing.implementation.common.LRUFiFoInstanceCache;
import main.java.processing.implementation.parsing.MOVINGParser;
import main.java.processing.implementation.preprocessing.*;
import main.java.processing.interfaces.IElementCache;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.*;
import java.util.*;

import static main.java.utils.MainUtils.*;

/**
 * Created by Blume Till on 02.03.2017.
 */
public class Main {
    private static final Logger logger = LogManager.getLogger(Main.class.getSimpleName());


    public static void main(String[] args) {
        //mute System errors from NxParser for normal procedure
        if (!logger.getLevel().isLessSpecificThan(Level.TRACE)) {
            try {
                System.setErr(new PrintStream(new FileOutputStream("system-errors.txt")));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        logger.debug(Arrays.toString(args));
        // JUL to slf4j logging bridge needed for nxparser logging
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        Options options = new Options();

        ///////////////////////////////////////////////////////////////////////

        ///////////////////////////////////////////////////////////////////////
        // read an input file
        Option file = new Option("f", "files", true, "read from file(s)");
        file.setArgName("file");
        file.setRequired(true);
        options.addOption(file);
        ///////////////////////////////////////////////////////////////////////
        Option fixLiterals = new Option("fb", "fixBlankNodes", false, "try to fix Blank Nodes");
        options.addOption(fixLiterals);
        ///////////////////////////////////////////////////////////////////////
        Option inputDirectoryFilter = new Option("if", "inputFilter", true, "regex pattern to filter filenames");
        inputDirectoryFilter.setArgName("inputFilter");
        options.addOption(inputDirectoryFilter);
        ///////////////////////////////////////////////////////////////////////
        Option memoryCacheSize = new Option("c", "cachesize", true, "instances stored in main memory");
        memoryCacheSize.setArgName("int");
        options.addOption(memoryCacheSize);
        ////////////
        Option deleteDiskCache = new Option("ddc", "deleteDiskCache", false, "Deletes the disk cache after processing. Caution, manually deleting in OS faster.");
        options.addOption(deleteDiskCache);
        ///////////////////////////////////////////////////////////////////
        // read mapping and query
        Option mapping = new Option("m", "mapping", true, "location of mapping file");
        mapping.setArgName("mapping");
        mapping.setRequired(true);
        options.addOption(mapping);
        ///////////////////////////////////////////////////////////////////
        Option inferenceOption = new Option("i", "inference", true, "activate inferencing");
        inferenceOption.setArgs(2);
        inferenceOption.setArgName("<mapping> <datasources>");
        options.addOption(inferenceOption);


        ///////////////////////////////////////////////////////////////////
        Option datasourceURIs = new Option("ds", "datasources", true, "location of datasource URIs (query LODatio if not provided)");
        options.addOption(datasourceURIs);
        ///////////////////////////////////////////////////////////////////
        Option usePLDs = new Option("pld", "usePLD", false, "harvest the complete pay-level domain (also exports simple harvesting)");
        options.addOption(usePLDs);
        ///////////////////////////////////////////////////////////////////


        ///////////////////////////////////////////////////////////////////
        //write output to folder
        OptionGroup outputGroup = new OptionGroup();
        outputGroup.setRequired(true);

        Option output = new Option("o", "output", true, "output folder");
        output.setArgName("folder");
        outputGroup.addOption(output);

        options.addOptionGroup(outputGroup);
        ///////////////////////////////////////////////////////////////////
        //print help menu
        Options optionsHelp = new Options();
        Option help = new Option("h", "help", false, "print help");
        optionsHelp.addOption(help);

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
        List<String> inputFiles = new LinkedList<>();
        String regexFileFilter;

        String outputDir = null;
        Mapping mapping = null;
        HashSet<String> datasourceURIs;
        boolean usePLDFilter = cmd.hasOption("pld");
        boolean useInferencing = cmd.hasOption("i");

        int cacheSize = Integer.MAX_VALUE;

        if (cmd.hasOption("c"))
            cacheSize = Integer.parseInt(cmd.getOptionValue("c"));

        //get input files
        if (cmd.hasOption("f")) {
            inputFiles.add(cmd.getOptionValue("f"));
            logger.debug("Using input file " + cmd.getOptionValue("f"));
        }

        //filter for specific files in folder
        if (cmd.hasOption("if")) {
            regexFileFilter = cmd.getOptionValue("if");
            logger.debug("using file filter...");
        } else
            regexFileFilter = ".*";

        final String finalRegex = regexFileFilter;
        FileFilter fileFilter = (pathname) -> (pathname != null ? pathname.toString().matches(finalRegex) : false);


        Mapping inferencedMapping = null;
        Set<String> inferencedDS = null;
        if (cmd.hasOption("m")) {
            mapping = new Mapping(readFile(cmd.getOptionValue("m")));
            if (cmd.hasOption("i")) {
//                inferencedMapping = LODatioQuery.mappingInferencing(mapping, cmd.getOptionValue("m"));
                String[] innerArgs = cmd.getOptionValues("i");
                inferencedMapping = new Mapping(readFile(innerArgs[0]));
                inferencedDS = loadContexts(innerArgs[1]);
                logger.debug("using inferenced mapping");
            } else
                logger.debug("using base mapping");
        }


        if (cmd.hasOption("ds")) {
            datasourceURIs = loadContexts(cmd.getOptionValue("ds"));
            logger.debug("Using datasource URIs from file ...");
        } else {
            //context file not given, then query LODatio
            LODatioQuery queryEngine = new LODatioQuery();
            Set<String> queryStrings = mapping.getQueries();
            datasourceURIs = new HashSet<>();
            Iterator<String> queryStringIterator = queryStrings.iterator();
            while (queryStringIterator.hasNext())
                datasourceURIs.addAll(queryEngine.queryDatasource(queryStringIterator.next(), -1));

        }
        if (cmd.hasOption("pld"))
            usePLDFilter = true;

        if (cmd.hasOption("o"))
            outputDir = cmd.getOptionValue("o");

        ///////////////////////////////////////////////////////////////////
        ///////////////////////////////////////////////////////////////////
        ///////////////////////////////////////////////////////////////////
        ///////////////////////////////////////////////////////////////////


        //source of RDF triples
        IQuintSource quintSource = null;
        if (!inputFiles.isEmpty()) {
            quintSource = new FileQuadSource(inputFiles, true,
                    "http://harverster.informatik.uni-kiel.de/", fileFilter);
        }

        //extract actual file name (without parent folders)
        String p = getFileName(inputFiles);


        BasicQuintPipeline preProcessingPipeline = new BasicQuintPipeline();


        //filter fix common mistakes
        preProcessingPipeline.addProcessor(new FixBNodes(cmd.hasOption("fb"), "http://harverster.informatik.uni-kiel.de/"));
        //reduce the number of instances
        if (usePLDFilter)
            preProcessingPipeline.addProcessor(new PLDFilter(datasourceURIs));
        else
            preProcessingPipeline.addProcessor(new ContextFilter(datasourceURIs));


        // all quints have to pass the pre-processing pipeline
        quintSource.registerQuintListener(preProcessingPipeline);

        //aggregate all quints that passed the pipeline to RDF Instances and add them to a cache
        IElementCache<IInstanceElement> rdfInstanceCache = new LRUFiFoInstanceCache<>(cacheSize, "disk-cache", cmd.hasOption("ddc"));
        InstanceAggregator instanceAggregatorContext = new InstanceAggregator(rdfInstanceCache);
        preProcessingPipeline.registerQuintListener(instanceAggregatorContext);


        //convert RDF instances to JSON instances
        MOVINGParser parserBaseContext = new MOVINGParser(mapping);
        MOVINGParser parserBasePLD = null;
        MOVINGParser parserInferContext = null;
        MOVINGParser parserInferPLD = null;

        if (useInferencing)
            parserInferContext = new MOVINGParser(inferencedMapping);
        if (usePLDFilter) {
            //convert RDF instances to JSON instances
            parserBasePLD = new MOVINGParser(mapping);
            if (useInferencing)
                parserInferPLD = new MOVINGParser(inferencedMapping);
        }


        //in-memory buffer to store converted JSON instances
        DataItemBuffer jsonBufferBaseContext = new DataItemBuffer();
        DataItemBuffer jsonBufferBasePLD = new DataItemBuffer();
        DataItemBuffer jsonBufferInferContext = new DataItemBuffer();
        DataItemBuffer jsonBufferInferPLD = new DataItemBuffer();


        //connect parser and json cache
        ContextHarvester harvesterBaseContext = new ContextHarvester(parserBaseContext, rdfInstanceCache, jsonBufferBaseContext, datasourceURIs);
        parserBaseContext.setHarvester(harvesterBaseContext);

        ContextHarvester harvesterInferContext = null;
        PLDHarvester harvesterBasePLD = null;
        PLDHarvester harvesterInferPLD = null;
        if (useInferencing) {
            harvesterInferContext = new ContextHarvester(parserInferContext, rdfInstanceCache, jsonBufferInferContext, inferencedDS);
            parserInferContext.setHarvester(harvesterInferContext);
        }
        if (usePLDFilter) {
            harvesterBasePLD = new PLDHarvester(parserBasePLD, rdfInstanceCache, jsonBufferBasePLD, datasourceURIs);
            parserBasePLD.setHarvester(harvesterBasePLD);
            if (useInferencing) {
                harvesterInferPLD = new PLDHarvester(parserInferPLD, rdfInstanceCache, jsonBufferInferPLD, inferencedDS);
                parserInferPLD.setHarvester(harvesterInferPLD);
            }
        }


        jsonBufferBaseContext.registerSink(new FileJSONSink(new PrintStream(
                outputDir + File.separator + "simple_harvesting-" + p + ".json")));
        if(useInferencing)
            jsonBufferInferContext.registerSink(new FileJSONSink(new PrintStream(
                    outputDir + File.separator + "simple_harvesting_inferencing-" + p + ".json")));
        if (usePLDFilter)
            jsonBufferBasePLD.registerSink(new FileJSONSink(new PrintStream(
                    outputDir + File.separator + "pld_harvesting-" + p + ".json")));
        if (usePLDFilter && useInferencing)
            jsonBufferInferPLD.registerSink(new FileJSONSink(new PrintStream(
                    outputDir + File.separator + "pld_harvesting_inferencing-" + p + ".json")));

        //listen to RDF instances
        rdfInstanceCache.registerCacheListener(harvesterBaseContext);
        if(useInferencing)
            rdfInstanceCache.registerCacheListener(harvesterInferContext);
        if (usePLDFilter)
            rdfInstanceCache.registerCacheListener(harvesterBasePLD);
        if (usePLDFilter && useInferencing)
            rdfInstanceCache.registerCacheListener(harvesterInferPLD);

        logger.info("Harvesting started ....");
        long startTime = System.currentTimeMillis();

        //starting the source starts all machinery
        quintSource.start();


//        try {
//            harvesterContext.join();
//            harvesterPLD.join();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        long endTime = System.currentTimeMillis();
        long time = ((endTime - startTime) / 1000) / 60;
        logger.info("Harvesting took: " + time + " min");


        /*
            export some statistics
         */
        String simpleHarvestingInfoString = "Simple Harvesting: " + parserBaseContext.getMissingConcept() + " Missing Concepts " +
                parserBaseContext.getMissingPerson() + " Missing Persons " +
                parserBaseContext.getMissingVenue() + " Missing Venue! " +
                (parserBaseContext.getMissingVenue() + parserBaseContext.getMissingPerson() + parserBaseContext.getMissingConcept()) + " total Cache Misses!";
        logger.info(simpleHarvestingInfoString);

        File fileContext = new File(outputDir + File.separator +
                "simple_harvesting-" + p + "_cache_misses.txt");
        try (FileWriter writer = new FileWriter(fileContext, false)) {
            PrintWriter pw = new PrintWriter(writer);
            pw.println(simpleHarvestingInfoString);
            pw.close();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        if (useInferencing){
            String simpleHarvestingInferencingInfoString = "Simple Harvesting: " + parserInferContext.getMissingConcept() + " Missing Concepts " +
                    parserInferContext.getMissingPerson() + " Missing Persons " +
                    parserInferContext.getMissingVenue() + " Missing Venue! " +
                    (parserInferContext.getMissingVenue() + parserInferContext.getMissingPerson() + parserInferContext.getMissingConcept()) + " total Cache Misses!";
            logger.info(simpleHarvestingInferencingInfoString);

            File fileInferContext = new File(outputDir + File.separator +
                    "simple_harvesting_inferencing-" + p + "_cache_misses.txt");
            try (FileWriter writer = new FileWriter(fileInferContext, false)) {
                PrintWriter pw = new PrintWriter(writer);
                pw.println(simpleHarvestingInferencingInfoString);
                pw.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }

        if (usePLDFilter) {
            String pldHarvestingInfoString = "PLD Harvesting: " + parserBasePLD.getMissingConcept() + " Missing Concepts " +
                    parserBasePLD.getMissingPerson() + " Missing Persons " +
                    parserBasePLD.getMissingVenue() + " Missing Venue! " +
                    (parserBasePLD.getMissingVenue() + parserBasePLD.getMissingPerson() + parserBasePLD.getMissingConcept()) + " total Cache Misses!";
            logger.info(pldHarvestingInfoString);

            File filePLD = new File(outputDir + File.separator +
                    "pld_harvesting-" + p + "_cache_misses.txt");

            try (FileWriter writer = new FileWriter(filePLD, false)) {
                PrintWriter pw = new PrintWriter(writer);
                pw.println(pldHarvestingInfoString);
                pw.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            if(useInferencing){
                String pldHarvestingInferencingInfoString = "PLD Harvesting: " + parserInferPLD.getMissingConcept() + " Missing Concepts " +
                        parserInferPLD.getMissingPerson() + " Missing Persons " +
                        parserInferPLD.getMissingVenue() + " Missing Venue! " +
                        (parserInferPLD.getMissingVenue() + parserInferPLD.getMissingPerson() + parserInferPLD.getMissingConcept()) + " total Cache Misses!";
                logger.info(pldHarvestingInferencingInfoString);

                File fileInferPLD = new File(outputDir + File.separator +
                        "pld_harvesting_inferencing-" + p + "_cache_misses.txt");

                try (FileWriter writer = new FileWriter(fileInferPLD, false)) {
                    PrintWriter pw = new PrintWriter(writer);
                    pw.println(pldHarvestingInferencingInfoString);
                    pw.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        //delete cache
        logger.debug("Deleting cache...");
        rdfInstanceCache.flush();
    }
}
