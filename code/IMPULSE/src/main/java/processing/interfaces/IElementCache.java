package main.java.processing.interfaces;

import main.java.common.interfaces.IInstanceElement;
import main.java.common.interfaces.ILocatable;
import main.java.common.interfaces.IResource;

import java.util.List;
import java.util.Set;


/**
 * A caching structure for instance data. The cache should obey the {@link Set}
 * contract in that there should be no duplicate entries. Instances are expected
 * to be identified by a given {@link IResource} locator, as specified in
 * {@link IInstanceElement}.
 * 
 * @author Bastian
 * 
 */
public interface IElementCache<T extends ILocatable> {

	/**
	 * Checks, whether a given instance is already in the cache
	 * 
	 * @param i
	 *            The instance to be checked for
	 * @return True, if the instance is already contained in the cache, false
	 *         otherwise
	 */
	boolean contains(T i);

	/**
	 * Checks, whether a given instance is already in the cache. The instance is
	 * specified by its unique {@link IResource} locator
	 * 
	 * @param res
	 *            The instance's locator to be checked for
	 * @return True, if the instance is already contained in the cache, false
	 *         otherwise
	 */
	boolean contains(IResource res);

	/**
	 * Returns the instance specified by the given {@link IResource} locator.
	 * 
	 * @param res
	 *            The instance's locator
	 * @return The instance specified by the locator, <code>null</code>
	 *         otherwise
	 */
	T get(IResource res);

	 List<String> get2();

	/**
	 * Returns the number of elements contained inside of the cache
	 * 
	 * @return The number of elements
	 */
	int size();

	/**
	 * Add a new instance to the cache. If an entry is already present, it will
	 * be replaced by the argument, so care should be taken to avoid multiple
	 * {@link IResource} locators evaluating as equal
	 * 
	 * @param i
	 *            The instance to be added
	 * @return
	 */
	void add(T i);

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
	void registerCacheListener(IElementCacheListener<T> listener);

	/**
	 * Signals that no more instances should be cached. Listeners should be
	 * notified and the cache should be flushed
	 */
	void close();
}
