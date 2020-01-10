package main.java.processing.implementation.preprocessing;

import main.java.common.implementation.RDFInstance;
import main.java.common.interfaces.IInstanceElement;
import main.java.common.interfaces.IQuint;
import main.java.input.interfaces.IQuintListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;

import static main.java.utils.MainUtils.createFile;

public class InstanceCounter implements IQuintListener {
    private static final Logger logger = LogManager.getLogger(InstanceCounter.class.getSimpleName());

    private PrintStream out;
    private Set<Integer> subjectHashes = new HashSet<>();

    public InstanceCounter(String outfile) throws FileNotFoundException {
        this.out = new PrintStream(new FileOutputStream(createFile(outfile)));;
    }
    @Override
    public void finishedQuint(IQuint quint) {
        subjectHashes.add(quint.getSubject().hashCode());
    }

    @Override
    public void microBatch() {

    }

    @Override
    public void finished() {

    }
}
