package main.java.output.interfaces;

import java.util.List;

public interface IJsonSink {


    /**
     * return true if successfully exported
     * @param jsonString
     * @return
     */
    boolean export(String jsonString);

    /**
     * returns the number of successfully exported
     * @param jsonStrings
     * @return
     */
    int bulkExport(List<String> jsonStrings);


    /**
     * close the output connection
     * @return
     */
    boolean close();
}
