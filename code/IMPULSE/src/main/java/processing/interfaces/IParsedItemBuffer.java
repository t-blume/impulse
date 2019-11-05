package main.java.processing.interfaces;

import main.java.output.interfaces.IJsonSink;

public interface IParsedItemBuffer<T> {

    /**
     * add Item to cache
     * @param item
     */
    void add(T item);

    /**
     * checks for the presence of the specific item
     *
     * @param item
     * @return
     */
    boolean contain(T item);

    /**
     * removes one item in a specified order
     *
     * @return
     */
    T removeNext();

    /**
     * Flushes all items to the registered sinks
     */
    void flush();


    /**
     * register a sink that exports the cache
     *
     * @param jsonSink
     */
    void registerSink(IJsonSink jsonSink);


    int size();


    void close();
}
