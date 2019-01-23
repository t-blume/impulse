package main.java.processing.interfaces;


public interface IElementCacheListener<T> {

	/**
	 * Callback function for instances leaving the cache
	 * 
	 * @param instance
	 *            The instance
	 */
	void elementFlushed(T instance);

	/**
	 * Signals that no more instances will follow
	 */
	void finished();
}
