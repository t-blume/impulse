package main.java;

import main.java.common.implementation.Mapping;
import main.java.common.interfaces.IInstanceElement;
import main.java.common.interfaces.IQuint;
import main.java.input.implementation.FileQuadSource;
import main.java.input.implementation.RDF4JQuadSource;
import main.java.input.interfaces.IQuintListener;
import main.java.input.interfaces.IQuintSource;
import main.java.output.implementation.Elastify;
import main.java.output.implementation.FileJSONSink;
import main.java.output.implementation.RDF4QuadSink;
import main.java.output.interfaces.IQuadSink;
import main.java.processing.implementation.Harvester;
import main.java.processing.implementation.LODatioQuery;
import main.java.processing.implementation.common.DataItemCache;
import main.java.processing.implementation.common.FifoInstanceCache;
import main.java.processing.implementation.parsing.MOVINGParser;
import main.java.processing.implementation.preprocessing.*;
import main.java.processing.interfaces.IElementCache;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.helper.StringUtil;
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
        options.addOption(file);

        String availableRepositories = "";
        for (int i = 0; i < RDFRepository.values().length - 1; i++)
            availableRepositories += "" + RDFRepository.values()[i] + " | ";
        availableRepositories += "" + RDFRepository.values()[RDFRepository.values().length - 1] + "";

        // or read a directory with input files
        Option repo = new Option("r", "repository", true, "repository: <" +
                availableRepositories + "> <URL> <ID>");

        repo.setArgs(3);
        repo.setArgName("<type> <URL> <ID>");
        options.addOption(repo);


        ///////////////////////////////////////////////////////////////////////
        Option fixLiterals = new Option("fl", "fix", false, "try to fix literals");
        options.addOption(fixLiterals);
        ///////////////////////////////////////////////////////////////////////
        Option inputDirectoryFilter = new Option("if", "inputFilter", true, "regex pattern to filter filenames");
        inputDirectoryFilter.setArgName("inputFilter");
        options.addOption(inputDirectoryFilter);
        ///////////////////////////////////////////////////////////////////////


        ///////////////////////////////////////////////////////////////////
        // read mapping and query
        Option mapping = new Option("m", "mapping", true, "location of mapping file");
        mapping.setArgName("mapping");

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

        Option load = new Option("l", "load", false, "load file(s) to repository");
        outputGroup.addOption(load);


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

        //used to load files and store them in repo
        boolean loadOnly = false;
        IQuadSink quadSink = null;


        //mute System errors from NxParser for normal procedure
//        if (!logger.getLevel().isLessSpecificThan(Level.TRACE))
//            System.setErr(new PrintStream(new OutputStream() {
//                @Override
//                public void write(int b) {
//                }
//            }));

        //get input files
        if (cmd.hasOption("f")) {
            inputFiles.add(cmd.getOptionValue("f"));
            logger.debug("Using input file " + cmd.getOptionValue("f"));
        }

        if (cmd.hasOption("r")) {
            String repoType = cmd.getOptionValues("r")[0];
            for (int i = 0; i < RDFRepository.values().length; i++)
                if (RDFRepository.values()[i].toString().equals(repoType))
                    rdfRepository = RDFRepository.values()[i];


            repositoryURL = cmd.getOptionValues("r")[1];
            repositoryID = cmd.getOptionValues("r")[2];
            logger.debug("Using repository (" + rdfRepository + "): " + repositoryURL + " (" + repositoryID + ")");
        }

        if (cmd.hasOption("l")) {
            //load and fill respository
            if (repositoryURL == null || repositoryID == null) {
                logger.error("No valid repository  found!");
                System.exit(-1);
            }
            loadOnly = true;
            logger.debug("Loading == " + loadOnly);
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
            logger.debug("using mapping file ...");

        }

        if (cmd.hasOption("mi")) {
            mapping = LODatioQuery.mappingInferencing(mapping, cmd.getOptionValue("m"));
            logger.debug("using mapping inferencing ...");

        }

        if (cmd.hasOption("dCM")) {
            downloadCacheMiss = true;
            logger.debug("download cache misses ...");

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
        if (!inputFiles.isEmpty())
            quintSource = new FileQuadSource(inputFiles, true,
                    "http://harverster.informatik.uni-kiel.de/", fileFilter);
        if (repositoryURL != null) {
            if (rdfRepository.equals(RDFRepository.RDF4J)) {
                if (loadOnly)
                    quadSink = new RDF4QuadSink(repositoryURL, repositoryID);
                else
                    quintSource = new RDF4JQuadSource(repositoryURL, repositoryID);
            }
        }

        if (quintSource == null) {
            logger.error("Invalid RDF source!");
            System.exit(-1);
        }

        BasicQuintPipeline preprocessingPipelineContext = new BasicQuintPipeline();
        BasicQuintPipeline preprocessingPipelinePLD = new BasicQuintPipeline();

        //extract actual file name
        String p = getFileName(inputFiles);


        /*
         * filter fix literals
         */
        preprocessingPipelineContext.addProcessor(new FixLiterals(cmd.hasOption("fl")));
        preprocessingPipelinePLD.addProcessor(new FixLiterals(cmd.hasOption("fl")));


        preprocessingPipelineContext.addProcessor(new ContextFilter(datasourceURIs));
        if (usePLDFilter)
            preprocessingPipelinePLD.addProcessor(new PLDFilter(datasourceURIs));

        // all quints have to pass the pre-processing pipeline
        quintSource.registerQuintListener(preprocessingPipelineContext);

        if (usePLDFilter)
            quintSource.registerQuintListener(preprocessingPipelinePLD);


        logger.debug("AASDA");
        /*
        two basic modes
         */
        if (loadOnly) {
            logger.debug("Starting in \"load\"-mode....");
            ////////////////////// <--------- load repository
            IQuadSink finalQuadSink = quadSink;

            preprocessingPipelineContext.registerQuintListener(new IQuintListener() {
                long success = 0;
                long failed = 0;

                @Override
                public void finishedQuint(IQuint i) {
                    if (finalQuadSink.addQuint(i))
                        success++;
                    else
                        failed++;
                }

                @Override
                public void microBatch() {
                    logger.warn("Micro Batch not supported!");
                }

                @Override
                public void finished() {
                    finalQuadSink.finished();
                    logger.info("Added " + success + " to repository, " + failed + " failed!");
                }
            });
            quintSource.start();

        } else {
            logger.debug("Starting in \"process\"-mode....");

            ////////////////////// <--------- use repository if applicable
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
            if (usePLDFilter)
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

                if (usePLDFilter) {
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

        /*
        start the source to start the pipeline
         */

            logger.info("Harvesting took: " + time + " min");
        }


    }
}
