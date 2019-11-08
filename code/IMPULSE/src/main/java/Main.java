package main.java;

import main.java.common.implementation.Mapping;
import main.java.common.interfaces.IInstanceElement;
import main.java.input.implementation.FileQuadSource;
import main.java.input.interfaces.IQuintSource;
import main.java.output.implementation.FileJSONSink;
import main.java.processing.implementation.Harvester;
import main.java.processing.implementation.LODatioQuery;
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
        Option inferenceOption = new Option("i", "inference", false, "activate inferencing");
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
        boolean usePLDFilter = false;

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


        if (cmd.hasOption("m")) {
            String mappingString = readFile(cmd.getOptionValue("m"));
            mapping = new Mapping(mappingString);
            if (cmd.hasOption("i")) {
                mapping = LODatioQuery.mappingInferencing(mapping, cmd.getOptionValue("m"));
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


        /*
        Prepare always both pipelines, use pld pipeline only when option is used.
         */

        BasicQuintPipeline preProcessingPipelineContext = new BasicQuintPipeline();
        BasicQuintPipeline preProcessingPipelinePLD = new BasicQuintPipeline();


        /*
         * filter fix common miastakes
         */
        preProcessingPipelineContext.addProcessor(new FixBNodes(cmd.hasOption("fb"), "http://harverster.informatik.uni-kiel.de/"));
        preProcessingPipelinePLD.addProcessor(new FixBNodes(cmd.hasOption("fb"), "http://harverster.informatik.uni-kiel.de/"));


        preProcessingPipelineContext.addProcessor(new ContextFilter(datasourceURIs));
        preProcessingPipelinePLD.addProcessor(new PLDFilter(datasourceURIs));


        // all quints have to pass the pre-processing pipeline
        quintSource.registerQuintListener(preProcessingPipelineContext);
        //when pld harvesting is used, connect PLD pipeline to quint source
        if (usePLDFilter)
            quintSource.registerQuintListener(preProcessingPipelinePLD);

        //aggregate all quints that passed the pipeline to RDF Instances and add them to a cache
        IElementCache<IInstanceElement> rdfInstanceCacheContext = new LRUFiFoInstanceCache<>(cacheSize, "disk-cache_simple", cmd.hasOption("ddc"));
        InstanceAggregator instanceAggregatorContext = new InstanceAggregator(rdfInstanceCacheContext);
        preProcessingPipelineContext.registerQuintListener(instanceAggregatorContext);


        //Optional: PLD
        IElementCache<IInstanceElement> rdfInstanceCachePLD = null;
        if (usePLDFilter) {
            //aggregate all quints that passed the pipeline to RDF Instances and add them to a cache
            rdfInstanceCachePLD = new LRUFiFoInstanceCache<>(cacheSize, "disk-cache_pld", cmd.hasOption("ddc"));
            InstanceAggregator instanceAggregatorPLD = new InstanceAggregator(rdfInstanceCachePLD);
            preProcessingPipelinePLD.registerQuintListener(instanceAggregatorPLD);
        }

        //convert RDF instances to JSON instances
        MOVINGParser parserContext = new MOVINGParser(rdfInstanceCacheContext, mapping);
        MOVINGParser parserPLD = null;
        if (usePLDFilter) {
            //convert RDF instances to JSON instances
            parserPLD = new MOVINGParser(rdfInstanceCachePLD, mapping);
        }

        //in-memory buffer to store converted JSON instances
        DataItemBuffer jsonBufferContext = new DataItemBuffer();
        DataItemBuffer jsonBufferPLD = null;
        if (usePLDFilter)
            jsonBufferPLD = new DataItemBuffer();


        jsonBufferContext.registerSink(new FileJSONSink(new PrintStream(
                outputDir + File.separator + "simple_harvesting-" + p + ".json")));
        if (usePLDFilter)
            jsonBufferPLD.registerSink(new FileJSONSink(new PrintStream(
                    outputDir + File.separator + "pld_harvesting-" + p + ".json")));


        //connect parser and json cache
        Harvester harvesterContext = new Harvester(parserContext, jsonBufferContext);
        Harvester harvesterPLD = null;
        if (usePLDFilter)
            harvesterPLD = new Harvester(parserPLD, jsonBufferPLD);

        //listen to RDF instances
        rdfInstanceCacheContext.registerCacheListener(harvesterContext);
        if (usePLDFilter)
            rdfInstanceCachePLD.registerCacheListener(harvesterPLD);


        logger.info("Harvesting started ....");
        long startTime = System.currentTimeMillis();

        //starting the source starts all machinery
        quintSource.start();

        long endTime = System.currentTimeMillis();
        long time = ((endTime - startTime) / 1000) / 60;
        logger.info("Harvesting took: " + time + " min");

        /*
            export some statistics
         */
        String simpleHarvestingInfoString = "Simple Harvesting: " + parserContext.getMissingConcept() + " Missing Concepts " +
                parserContext.getMissingPerson() + " Missing Persons " +
                parserContext.getMissingVenue() + " Missing Venue! " +
                (parserContext.getMissingVenue() + parserContext.getMissingPerson() + parserContext.getMissingConcept()) + " total Cachemisses!";
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

        if (usePLDFilter) {
            String pldHarvestingInfoString = "PLD Harvesting: " + parserPLD.getMissingConcept() + " Missing Concepts " +
                    parserPLD.getMissingPerson() + " Missing Persons " +
                    parserPLD.getMissingVenue() + " Missing Venue! " +
                    (parserPLD.getMissingVenue() + parserPLD.getMissingPerson() + parserPLD.getMissingConcept()) + " total Cachemisses!";
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
        }
    }
}
