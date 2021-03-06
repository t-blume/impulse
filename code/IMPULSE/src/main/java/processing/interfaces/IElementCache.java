package main.java.processing.interfaces;

import main.java.common.interfaces.IInstanceElement;
import main.java.utils.LongQueue;

import java.util.Set;


/**
 * A caching structure for instance data. The cache should obey the {@link Set}
 * contract in that there should be no duplicate entries. Instances are expected
 * to be identified by a given locator, as specified in
 * {@link IInstanceElement}.
 * 
 * @author Bastian
 * 
 */
public interface IElementCache<T> {


	void setFifoQueue(LongQueue<Integer> fifoQueue);
	/**
	 * Checks, whether a given instance is already in the cache
	 * 
	 * @param element
	 *            The instance to be checked for
	 * @return True, if the instance is already contained in the cache, false
	 *         otherwise
	 */
	boolean contains(T element);

	/**
	 * Checks, whether a given instance is already in the cache. The instance is
	 * specified by its unique locator
	 * 
	 * @param locator
	 *            The instance's locator to be checked for
	 * @return True, if the instance is already contained in the cache, false
	 *         otherwise
	 */
	boolean contains(Integer locator);

	/**
	 * Returns the instance specified by the given locator.
	 * 
	 * @param locator
	 *            The instance's locator
	 * @return The instance specified by the locator, <code>null</code>
	 *         otherwise
	 */
	T get(Integer locator);

	 //List<String> get2();

	/**
	 * Returns the number of elements contained inside of the cache
	 * 
	 * @return The number of elements
	 */
	long size();

	/**
	 * Add a new instance to the cache. If an entry is already present, it will
	 * be replaced by the argument, so care should be taken to avoid multiple
	 * {@link Integer} locators evaluating as equal
	 * 
	 * @param element
	 *            The instance to be added
	 * @return
	 */
	void add(T element);

	/**
	 * Flushes the whole cache. That means, that all elements have to be removed
	 * from it, triggering the listener callbacks
	 */
	void flush();

	/**
	 * Registers an {@link IElementCacheListener}. It will be called for each
	 * instance removed from the cache
	 * 
	 * @param listener
	 *            The listener to be registered
	 */
	void registerCacheListener(IElementCacheListener listener);

	/**
	 * Signals that no more instances should be cached. Listeners should be
	 * notified and the cache should be flushed
	 */
	void close();


	void flushAll(String outfile);
}
