package main.java.input.interfaces;


import main.java.common.interfaces.IQuint;

/**
 * Listener used for handling processed {@link IQuint}s. The
 * {@link #finishedQuint(IQuint)} method will be called whenever an
 * {@link IQuint} has finished the processing stage.
 *
 * 
 * @author Bastian
 * 
 */
public interface IQuintListener {

	/**
	 * This method will be called every time, an {@link IQuint} finishes
	 * processing
	 * 
	 * @param quint
	 *            The finished quint
	 */
	void finishedQuint(IQuint quint);

	/**
	 * Signals that the source has completed a micro batch
	 */
	void microBatch();


	/**
	 * Signals that no more quints will follow
	 */
	void finished();
}
