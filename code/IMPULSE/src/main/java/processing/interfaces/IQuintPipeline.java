package main.java.processing.interfaces;

import main.java.common.interfaces.IQuint;
import main.java.input.interfaces.IQuintListener;

import java.util.List;

/**
 * A pipeline of {@link IQuintProcessor}s. A given {@link IQuint} will be
 * sequentially processed by a set number of processors. Registered listeners
 * will be informed of each thusly produced {@link IQuint}.
 * 
 * @author Bastian
 * 
 */
public interface IQuintPipeline {

	/**
	 * Processes the given {@link IQuint}. The quint will be used as input for the
	 * first processor, if any is registered. The output will be used as input
	 * for the next one. This will be repeated for each of the following
	 * processors. If there is no one left, the output will be send to the
	 * registered listeners
	 * 
	 * @param i
	 *            The quint to be processed
	 */
	void process(IQuint i);

	/**
	 * Add a processor. The order in which the processors were added should be
	 * used when processing incoming quints. The concrete implementation may
	 * choose not to implement modifications to the pipeline
	 * 
	 * @param p
	 *            The processor to be added
	 */
	void addProcessor(IQuintProcessor p);

	/**
	 * Remove a processor. The processor next in line will fill the space
	 * left.The concrete implementation may choose not to implement
	 * modifications to the pipeline
	 * 
	 * @param p
	 *            The processor to be removed.
	 */
	void removeProcessor(IQuintProcessor p);

	/**
	 * Produces a {@link List}-view of the pipeline. The order of the list
	 * should be as specified in the {@link #addProcessor(IQuintProcessor)}
	 * method
	 * 
	 * @return A list-view of the pipeline
	 */
	List<IQuintProcessor> getPipeline();

	/**
	 * Registers an {@link IQuintListener} which is notified whenever an
	 * {@link IQuint} leaves the pipeline
	 * 
	 * @param l
	 *            The listener to be registered
	 */
	void registerQuintListener(IQuintListener l);
}
