package main.java.processing.implementation.common;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import main.java.common.data.model.DataItem;
import main.java.output.interfaces.IJsonSink;
import main.java.processing.interfaces.IParsedItemCache;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Blume Till on 02.03.2017.
 */
public class DataItemCache implements IParsedItemCache<DataItem> {

    private List<IJsonSink> sinks;
    private LinkedHashSet<DataItem> set;

    public DataItemCache() {
//        set = HashObjSets.newUpdatableSet();
        set = new LinkedHashSet<>();
        sinks = new LinkedList<>();
    }

    @Override
    public void add(DataItem item) {
        if (item.getMetadataPersons() != null)
            set.add(item);
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
        set.forEach(D -> {
            try {
                exportList.add(mapper.writeValueAsString(D));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        });
        sinks.forEach(S -> {
            S.bulkExport(exportList);
            S.close();
        });
    }

    @Override
    public void registerSink(IJsonSink jsonSink) {
        sinks.add(jsonSink);
    }

    @Override
    public int size() {
        return set.size();
    }
}
