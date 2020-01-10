package main;

import main.java.common.interfaces.IInstanceElement;
import main.java.input.implementation.FileQuadSource;
import main.java.input.interfaces.IQuintSource;
import main.java.processing.implementation.common.LRUFiFoInstanceCache;
import main.java.processing.implementation.preprocessing.BasicQuintPipeline;
import main.java.processing.implementation.preprocessing.FixBNodes;
import main.java.processing.implementation.preprocessing.InstanceAggregator;
import main.java.processing.implementation.preprocessing.InstanceCounter;
import main.java.utils.MainUtils;
import main.java.utils.MemoryTracker;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;

public class Preparator {
    private static final Logger logger = LogManager.getLogger(Preparator.class.getSimpleName());

    public void runThisShit(List<String> inputFiles, int cacheSize, String diskCache, String outfile) throws FileNotFoundException {
        IQuintSource quintSource = new FileQuadSource(inputFiles, true, "http://harverster.informatik.uni-kiel.de/");
        BasicQuintPipeline preProcessingPipeline = new BasicQuintPipeline();
        //filter fix common mistakes
        preProcessingPipeline.addProcessor(new FixBNodes(true, "http://harverster.informatik.uni-kiel.de/"));
        // all quints have to pass the pre-processing pipeline
        quintSource.registerQuintListener(preProcessingPipeline);

        LRUFiFoInstanceCache<IInstanceElement> rdfInstanceCache = new LRUFiFoInstanceCache<>(cacheSize, diskCache, false);
        InstanceAggregator instanceAggregatorContext = new InstanceAggregator(rdfInstanceCache);
        preProcessingPipeline.registerQuintListener(instanceAggregatorContext);

        MemoryTracker memoryTracker = new MemoryTracker("stats", 500000);
        preProcessingPipeline.registerQuintListener(memoryTracker);




        logger.info("Harvesting started ....");
        long startTime = System.currentTimeMillis();
        //starting the source starts all machinery
        quintSource.start();
        rdfInstanceCache.flushAllToDisk(outfile);
        long endTime = System.currentTimeMillis();
        long time = endTime - startTime;
        logger.info("Kontrollsumme: " + time);
        logger.info("Harvesting took: " + MainUtils.prettyPrintTimeStamp(time));
    }


    public static void main(String[] args){
        //mute System errors from NxParser for normal procedure
        if (!logger.getLevel().isLessSpecificThan(Level.TRACE)) {
            try {
                System.setErr(new PrintStream(new FileOutputStream("system-errors.txt")));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        Preparator preparator = new Preparator();
        List<String> filenames = new LinkedList<>();
        filenames.add("testresources/sample-rdf-data.nt.gz");

        try {
            preparator.runThisShit(filenames, 100000, "../disk-storage", "../subjectHashes.csv");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

}
