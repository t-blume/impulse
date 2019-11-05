package main.java.output.implementation;


import main.java.output.interfaces.IJsonSink;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.PrintStream;
import java.util.List;


/**
 * Created by Blume Till on 07.10.2016.
 */
public class FileJSONSink implements IJsonSink {
    private static final Logger logger = LogManager.getLogger(FileJSONSink.class.getSimpleName());
    private static final int logger_interval = 1000;

    private final PrintStream pw;
    private int count = 0;

    public FileJSONSink(PrintStream pw) {
        this.pw = pw;
    }

    @Override
    public boolean close() {
        pw.close();
        logger.info("Exported " + count + " data items!");
        return true;
    }

    @Override
    public boolean export(String jsonString) {
        if (jsonString == null || jsonString.isEmpty())
            return false;

        count++;
        if (count % logger_interval == 0)
            logger.debug("Printed data item: " + count + "\r");

        if (count > 1)
            pw.println(jsonString);
        return true;
    }

    @Override
    public int bulkExport(List<String> jsonStrings) {
        int successful = 0;
        if (jsonStrings == null || jsonStrings.isEmpty())
            return successful;


        for (String jsonString : jsonStrings)
            successful = export(jsonString) ? successful++ : successful;

        return successful;
    }
}
