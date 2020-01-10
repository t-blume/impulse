package main.java.processing.interfaces;


import main.java.utils.LongQueue;

public interface IElementCacheListener{


	/**
	 * Signals that cache is full, parsing can start
	 */
	void startWorking(LongQueue<Integer> fifoQueue);


	void join() throws InterruptedException;
}
