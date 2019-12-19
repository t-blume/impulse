package main.java.processing.implementation.common;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import main.java.common.data.model.DataItem;
import main.java.output.interfaces.IJsonSink;
import main.java.processing.interfaces.IParsedItemBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Blume Till on 02.03.2017.
 */
public class DataItemBuffer implements IParsedItemBuffer<DataItem> {
    private static final Logger logger = LogManager.getLogger(DataItemBuffer.class.getSimpleName());

    private int capacity;
    private List<IJsonSink> sinks;
    private LinkedHashSet<DataItem> set;


    public DataItemBuffer() {
        this(5000);
    }

    public DataItemBuffer(int capacity) {
        this.capacity = capacity;
        set = new LinkedHashSet<>();
        sinks = new LinkedList<>();
    }


    @Override
    public void add(DataItem item) {
        if (item.getMetadataPersons() != null)
            set.add(item);
        if (set.size() >= capacity) {
            logger.info("Flushing intermediate chunk of " + set.size() + " items to sinks!");
            flush();
        }
    }

    @Override
    public boolean contain(DataItem item) {
        return set.contains(item);
    }

    @Override
    public DataItem removeNext() {
        return set.iterator().hasNext() ? set.iterator().next() : null;
    }

    @Override
    public void flush() {
        List<String> exportList = new LinkedList<>();
        ObjectMapper mapper = new ObjectMapper();

        //get all data items from set and parse to json string
        set.forEach(D -> {
            try {
                exportList.add(mapper.writeValueAsString(D));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        });
        //flush json strings to sinks
        sinks.forEach(S -> S.bulkExport(exportList));
        set = new LinkedHashSet<>();
    }

    @Override
    public void registerSink(IJsonSink jsonSink) {
        sinks.add(jsonSink);
    }

    @Override
    public int size() {
        return set.size();
    }

    @Override
    public void close() {
        sinks.forEach(S -> {
            S.close();
        });
    }
}
