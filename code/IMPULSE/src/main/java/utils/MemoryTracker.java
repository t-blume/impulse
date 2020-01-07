package main.java.utils;

import main.java.common.interfaces.IQuint;
import main.java.input.interfaces.IQuintListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;

import static main.java.utils.MainUtils.createFile;

public class MemoryTracker implements IQuintListener {
    private static final Logger logger = LogManager.getLogger(MemoryTracker.class.getSimpleName());
    private int loggingInterval = 50000000;
    private PrintStream out;
    private long n = 0;
    private long lastTime = System.currentTimeMillis();
    private long start = System.currentTimeMillis();

    private double maxTime = Double.MIN_VALUE;
    private double minTime = Double.MAX_VALUE;

    private long maxMemory = 0;
    private long maxUsedMemory = 0;

    public PrintStream getOut() {
        return out;
    }

    public MemoryTracker(){
        out = System.out;
    }
    public MemoryTracker(String output, int loggingInterval) {
        try {
            out = new PrintStream(new FileOutputStream(createFile(output + File.separator + "memory-tracker.txt")));
        } catch (FileNotFoundException e) {
            logger.warn("Cannot create output stream to " + output + File.separator + "memory-tracker.txt\n Using system out instead.");
            out = System.out;
        }
        this.loggingInterval = loggingInterval;
    }



    @Override
    public void finishedQuint(IQuint i) {
        n++;
        if (n % loggingInterval == 0) {
            long currentTime = System.currentTimeMillis();
            long delta = currentTime - lastTime;
            lastTime = currentTime;
            double instancesPerSecond = ((double) loggingInterval / delta * 1000.0);
            maxTime = Math.max(instancesPerSecond, maxTime);
            minTime = Math.min(instancesPerSecond, minTime);

            long runtimeMaxMemory = getCurrentlyMaxMemory();
            long runtimeUsedMemory = getReallyUsedMemory();

            maxMemory = Math.max(runtimeMaxMemory, maxMemory);
            maxUsedMemory = Math.max(maxUsedMemory, runtimeUsedMemory);
            logger.info("--------------------------------------");
            logger.info("Quads count: " + String.format("%,d", n));
            logger.info("Quads per second: " + String.format("%,d", (int) instancesPerSecond));
            logger.info("Available Memory: " + String.format("%,d", runtimeMaxMemory / 1024 / 1024) + " MB");
            logger.info("Used Memory: " + String.format("%,d", runtimeUsedMemory / 1024 / 1024) + " MB");

        }
    }

    @Override
    public void microBatch() {

    }

    @Override
    public void finished() {
        out.println("--------Quads---------");
        out.println("Num: " + String.format("%,d", n));
        out.println("Time: " + String.format("%,d", (int) (System.currentTimeMillis() - start) / 1000) + "s");
        out.println("Maximum quads per second: " + String.format("%,d", (int) maxTime));
        out.println("Minimum quads per second: " + String.format("%,d", (int) minTime));
        out.println("Maximum memory allocated: " + String.format("%,d", maxMemory / 1024 / 1024) + " MB");
        out.println("Maximum memory used: " + String.format("%,d", maxUsedMemory / 1024 / 1024) + " MB");
        out.println("--------------------------");
        logger.info("--------Quads---------");
        logger.info("Num: " + String.format("%,d", n));
        logger.info("Time: " + String.format("%,d", (int) (System.currentTimeMillis() - start) / 1000) + "s");
        logger.info("Maximum quads per second: " + String.format("%,d", (int) maxTime));
        logger.info("Minimum quads per second: " + String.format("%,d", (int) minTime));
        logger.info("Maximum memory allocated: " + String.format("%,d", maxMemory / 1024 / 1024) + " MB");
        logger.info("Maximum memory used: " + String.format("%,d", maxUsedMemory / 1024 / 1024) + " MB");
        logger.info("--------------------------");
    }


    ////DIRTY LITTLE HELPER
    public long getReallyUsedMemory() {
        long before = getGcCount();
        System.gc();
        while (getGcCount() == before) ;
        return getCurrentlyUsedMemory();
    }

    public long getGcCount() {
        long sum = 0;
        for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) {
            long count = b.getCollectionCount();
            if (count != -1) {
                sum += count;
            }
        }
        return sum;
    }

    public long getCurrentlyUsedMemory() {
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() +
                        ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage().getUsed();
    }

    public long getCurrentlyMaxMemory() {
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax() +
                        ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage().getMax();
    }
}
