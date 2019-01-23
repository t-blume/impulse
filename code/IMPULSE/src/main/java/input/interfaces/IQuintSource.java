package main.java.input.interfaces;


import main.java.common.interfaces.IQuint;

import java.io.IOException;

/**
 * A stream of {@link IQuint}s. The source is expected to start working after
 * the {@link #start()} method has been called. Before the first quint is
 * produced, the source has to notify its listeners by calling their
 * {@link IQuintSourceListener#sourceStarted()} method. For each produced quint,
 * the {@link IQuintSourceListener#pushedQuint(IQuint)} method should be called.
 * When the source closed down and no more quints will be produced, the
 * listeners should be notified by calling
 * {@link IQuintSourceListener#sourceClosed()}
 * 
 * @author Bastian
 * 
 */


public interface IQuintSource {

	/**
	 * Closes the source, i.e. frees up any used resources
	 */
	void close() throws IOException;

	/**
	 * Initializes the necessary state of the {@link IQuintSource}. Afterwards,
	 * the source is required to be able to stream quints
	 */
	void start();

	/**
	 * Registers an {@link IQuintListener} which will be notified for each
	 * produced quint
	 * 
	 * @param listener
	 *            The listener to be registered
	 */
	void registerQuintListener(IQuintSourceListener listener);

	/**
	 * Removes a registered {@link IQuintListener}, if it matches the given one.
	 * Otherwise no changes will happen
	 * 
	 * @param listener
	 *            The listener to be removed
	 */
	void removeQuintListener(IQuintSourceListener listener);

}
