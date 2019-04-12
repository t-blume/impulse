package main.java;

import main.java.common.implementation.Mapping;
import main.java.common.interfaces.IInstanceElement;
import main.java.input.implementation.FileQuadSource;
import main.java.input.implementation.RDF4JQuadSource;
import main.java.input.interfaces.IQuintSource;
import main.java.output.implementation.Elastify;
import main.java.output.implementation.FileJSONSink;
import main.java.processing.implementation.Harvester;
import main.java.processing.implementation.LODatioQuery;
import main.java.processing.implementation.common.DataItemCache;
import main.java.processing.implementation.common.FifoInstanceCache;
import main.java.processing.implementation.parsing.MOVINGParser;
import main.java.processing.implementation.preprocessing.BasicQuintPipeline;
import main.java.processing.implementation.preprocessing.ContextFilter;
import main.java.processing.implementation.preprocessing.InstanceAggregator;
import main.java.processing.implementation.preprocessing.PLDFilter;
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

    public enum RDFRepository {
        RDF4J
    }

    public static void main(String[] args) {
        // JUL to slf4j logging bridge needed for nxparser logging
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        Options options = new Options();


        ///////////////////////////////////////////////////////////////////////
        OptionGroup inputGroup = new OptionGroup();
        inputGroup.setRequired(true);
        // read an input file
        Option file = new Option("f", "file(s)", true, "read from file(s)");
        file.setArgName("file");
        inputGroup.addOption(file);


        String availableRepositories = "";
        for (int i = 0; i < RDFRepository.values().length - 1; i++)
            availableRepositories += "" + RDFRepository.values()[i] + " | ";
        availableRepositories += "" + RDFRepository.values()[RDFRepository.values().length - 1] + "";

        // or read a directory with input files
        Option repo = new Option("r", "repository", true, "read from repository: <" +
                availableRepositories + "> <URL>");

        //TODO what is Type? Is there any need of the repo ID?
        repo.setArgs(2);
        repo.setArgName("<type> <URL>");
        inputGroup.addOption(repo);

        inputGroup.setRequired(true);

        //add option
        options.addOptionGroup(inputGroup);
        ///////////////////////////////////////////////////////////////////////

        ///////////////////////////////////////////////////////////////////////
        Option inputDirectoryFilter = new Option("if", "inputFilter", true, "regex pattern to filter filenames");
        inputDirectoryFilter.setArgName("inputFilter");
        options.addOption(inputDirectoryFilter);
        ///////////////////////////////////////////////////////////////////////


        ///////////////////////////////////////////////////////////////////
        // read mapping and query
        Option mapping = new Option("m", "mapping", true, "location of mapping file");
        mapping.setArgName("mapping");
        mapping.setRequired(true);
        options.addOption(mapping);

        Option mappingInferencing = new Option("mi", "mappingInferencing", false, "inferencing of mapping file");
        mapping.setArgName("mappingInferencing");
        //mapping.setRequired(true);
        options.addOption(mappingInferencing);


        ///////////////////////////////////////////////////////////////////

        ///////////////////////////////////////////////////////////////////
        Option datasourceURIs = new Option("ds", "datasources", true, "location of datasource URIs");
        options.addOption(datasourceURIs);
        ///////////////////////////////////////////////////////////////////
        Option usePLDs = new Option("pld", "usePLD", false, "harvest the complete pay-level domain");
        options.addOption(usePLDs);
        ///////////////////////////////////////////////////////////////////


        ///////////////////////////////////////////////////////////////////
        //write output to folder
        OptionGroup outputGroup = new OptionGroup();
        outputGroup.setRequired(true);

        Option output = new Option("o", "output", true, "output folder");
        output.setArgName("folder");
        outputGroup.addOption(output);

        Option elastify = new Option("e", "elastify", true, "Elasticsearch index");
        elastify.setArgs(2);
        elastify.setArgName("<index> <type>");
        outputGroup.addOption(elastify);
        options.addOptionGroup(outputGroup);
        ///////////////////////////////////////////////////////////////////


        Option downloadCacheMiss = new Option("dCM", "downloadCacheMisses", false, "Download the missing Resources");
        downloadCacheMiss.setArgName("downloadCacheMiss");
        //mapping.setRequired(true);
        options.addOption(downloadCacheMiss);


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
        String elasticIndex = null;
        String elasticType = null;
        Mapping mapping = null;
        HashSet<String> datasourceURIs;
        boolean usePLDFilter = false;
        boolean downloadCacheMiss = false;

        //repository input
        RDFRepository rdfRepository = null;
        String repositoryURL = null;
        String repositoryID = null;
        //mute System errors from NxParser for normal procedure
        if (!logger.getLevel().isLessSpecificThan(Level.TRACE))
            System.setErr(new PrintStream(new OutputStream() {
                @Override
                public void write(int b) {
                }
            }));

        //get input files
        if (cmd.hasOption("f"))
            inputFiles.add(cmd.getOptionValue("f"));
        else if (cmd.hasOption("r")) {
            //TODO
            String repoType = cmd.getOptionValues("r")[0];
            for (int i = 0; i < RDFRepository.values().length - 1; i++)
                if(RDFRepository.values()[i].toString().equals(repoType))
                    rdfRepository = RDFRepository.values()[i];

            repositoryURL = cmd.getOptionValues("r")[1];
        }

        //filter for specific files in folder
        if (cmd.hasOption("if"))
            regexFileFilter = cmd.getOptionValue("if");
        else
            regexFileFilter = ".*";

        final String finalRegex = regexFileFilter;
        FileFilter fileFilter = (pathname) -> (pathname != null ? pathname.toString().matches(finalRegex) : false);


        if (cmd.hasOption("m")) {
            String mappingString = readFile(cmd.getOptionValue("m"));
            mapping = new Mapping(mappingString);
        }

        if (cmd.hasOption("mi")) {
            mapping = LODatioQuery.mappingInferencing(mapping, cmd.getOptionValue("m"));
        }

        if (cmd.hasOption("dCM")) {
            downloadCacheMiss = true;
        }

        if (cmd.hasOption("ds"))
            datasourceURIs = loadContexts(cmd.getOptionValue("ds"));
        else {
            //context file not given, then query LODatio
            LODatioQuery queryEngine = new LODatioQuery();
            Set<String> queryStrings = mapping.getQueries();
            datasourceURIs = new HashSet<>();
            Iterator<String> queryStringIterator = queryStrings.iterator();
            while (queryStringIterator.hasNext()) {
                datasourceURIs.addAll(queryEngine.queryDatasource(queryStringIterator.next(), -1));
            }
        }
        if (cmd.hasOption("pld"))
            usePLDFilter = true;

        if (cmd.hasOption("o"))
            outputDir = cmd.getOptionValue("o");
        else if (cmd.hasOption("e")) {
            String[] args = cmd.getOptionValues("e");
            elasticIndex = args[0];
            elasticType = args[1];
        }


        ///////////////////////////////////////////////////////////////////
        ///////////////////////////////////////////////////////////////////
        ///////////////////////////////////////////////////////////////////
        ///////////////////////////////////////////////////////////////////


        //source of RDF triples
        IQuintSource quintSource = null;
        if(!inputFiles.isEmpty())
            quintSource = new FileQuadSource(inputFiles, true,
                "http://harverster.informatik.uni-kiel.de/", fileFilter);
        if(repositoryURL != null){
            if(rdfRepository.equals(RDFRepository.RDF4J))
                quintSource = new RDF4JQuadSource(repositoryURL, repositoryID);
        }

        if(quintSource == null){
            logger.error("Invalid RDF source!");
            System.exit(-1);
        }

        BasicQuintPipeline preprocessingPipelineContext = new BasicQuintPipeline();
        BasicQuintPipeline preprocessingPipelinePLD = new BasicQuintPipeline();

        //extract actual file name
        String p = getFileName(inputFiles);


        preprocessingPipelineContext.addProcessor(new ContextFilter(datasourceURIs));
        if (usePLDFilter)
            preprocessingPipelinePLD.addProcessor(new PLDFilter(datasourceURIs));

        // all quints have to pass the pre-processing pipeline
        quintSource.registerQuintListener(preprocessingPipelineContext);

        if (usePLDFilter)
            quintSource.registerQuintListener(preprocessingPipelinePLD);


        //aggregate all quints that passed the pipeline to RDF Instances and add them to a cache
        IElementCache<IInstanceElement> rdfInstanceCacheContext = new FifoInstanceCache<>();
        InstanceAggregator instanceAggregatorContext = new InstanceAggregator(rdfInstanceCacheContext);
        preprocessingPipelineContext.registerQuintListener(instanceAggregatorContext);

        IElementCache<IInstanceElement> rdfInstanceCachePLD = null;
        if (usePLDFilter) {
            //aggregate all quints that passed the pipeline to RDF Instances and add them to a cache
            rdfInstanceCachePLD = new FifoInstanceCache<>();
            InstanceAggregator instanceAggregatorPLD = new InstanceAggregator(rdfInstanceCachePLD);
            preprocessingPipelinePLD.registerQuintListener(instanceAggregatorPLD);
        }


        //convert RDF instances to JSON instances
        MOVINGParser parserContext = new MOVINGParser(rdfInstanceCacheContext, mapping, downloadCacheMiss);
        MOVINGParser parserPLD = null;
        if (usePLDFilter) {
            //convert RDF instances to JSON instances
            parserPLD = new MOVINGParser(rdfInstanceCachePLD, mapping, downloadCacheMiss);
        }

        //in-memory cache to store converted JSON instances
        DataItemCache jsonCacheContext = new DataItemCache();
        DataItemCache jsonCachePLD = null;
        if (usePLDFilter)
            jsonCachePLD = new DataItemCache();

        if (outputDir != null) {
            jsonCacheContext.registerSink(new FileJSONSink(new PrintStream(
                    outputDir + File.separator + "context-" + p + ".json")));
            if (usePLDFilter) {
                jsonCachePLD.registerSink(new FileJSONSink(new PrintStream(
                        outputDir + File.separator + "pld-" + p + ".json")));
            }
        } else if (elasticIndex != null) {
            jsonCacheContext.registerSink(new Elastify(elasticIndex, elasticType));
            if (usePLDFilter) {
                logger.error("PLD and ELasticsearch not yet supported!");
                System.exit(-1);
            }
        } else
            logger.error("Misconfiguration export!");
        //combine parser and json cache
        Harvester harvesterContext = new Harvester(parserContext, jsonCacheContext);
        Harvester harvesterPLD = null;
        if (usePLDFilter)
            harvesterPLD = new Harvester(parserPLD, jsonCachePLD);

        //listen to RDF instances
        rdfInstanceCacheContext.registerCacheListener(harvesterContext);
        if(usePLDFilter)
            rdfInstanceCachePLD.registerCacheListener(harvesterPLD);

        long startTime = System.currentTimeMillis();
        quintSource.start();

        long endTime = System.currentTimeMillis();
        long time = ((endTime - startTime) / 1000) / 60;

        //Export dump.nt

        logger.info(parserContext.getMissingConcept() + " Missing Concepts " +
                parserContext.getMissingPerson() + " Missing Persons " +
                parserContext.getMissingVenue() + " Missing Venue! " +
                (parserContext.getMissingVenue() + parserContext.getMissingPerson() + parserContext.getMissingConcept()) + " total Cachemisses!");

        logger.info(parserPLD.getMissingConcept() + " Missing Concepts " +
                parserPLD.getMissingPerson() + " Missing Persons " +
                parserPLD.getMissingVenue() + " Missing Venue! " +
                (parserPLD.getMissingVenue() + parserPLD.getMissingPerson() + parserPLD.getMissingConcept()) + " total Cachemisses!");


        File fileContext = new File(outputDir + File.separator +
                "context-" + p + "_cachemiss.txt");

        try (FileWriter writer = new FileWriter(fileContext, false)) {
            PrintWriter pw = new PrintWriter(writer);
            pw.println(parserContext.getMissingConcept() + " Missing Concepts " +
                    parserContext.getMissingPerson() + " Missing Persons " +
                    parserContext.getMissingVenue() + " Missing Venue! " +
                    (parserContext.getMissingVenue() + parserContext.getMissingPerson() + parserContext.getMissingConcept()) + " total Cachemisses!");

        } catch (IOException e1) {
            e1.printStackTrace();
        }

        File filePLD = new File(outputDir + File.separator +
                "pld-" + p + "_cachemiss.txt");

        try (FileWriter writer = new FileWriter(filePLD, false)) {
            PrintWriter pw = new PrintWriter(writer);
            pw.println(parserPLD.getMissingConcept() + " Missing Concepts " +
                    parserPLD.getMissingPerson() + " Missing Persons " +
                    parserPLD.getMissingVenue() + " Missing Venue! " +
                    (parserPLD.getMissingVenue() + parserPLD.getMissingPerson() + parserPLD.getMissingConcept()) + " total Cachemisses!");

        } catch (IOException e1) {
            e1.printStackTrace();
        }


        if (downloadCacheMiss) {
            File file = new File(outputDir + File.separator +
                    "context-" + p + "_cachemiss-dump.nt");

            try (FileWriter writer = new FileWriter(file, true)) {
                PrintWriter pw = new PrintWriter(writer);
                parserContext.newData.forEach(x -> pw.println(x));

            } catch (IOException e1) {
                e1.printStackTrace();
            }

            if(usePLDFilter){
                File filePLD2 = new File(outputDir + File.separator +
                        "pld-" + p + "_cachemiss-dump.nt");

                try (FileWriter writer = new FileWriter(filePLD2, true)) {
                    PrintWriter pw = new PrintWriter(writer);
                    parserPLD.newData.forEach(x -> pw.println(x));

                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }

        }
        logger.info("Harvesting took: " + time + " min");
    }
}
