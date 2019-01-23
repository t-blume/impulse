package main.java.input.interfaces;


import main.java.common.interfaces.IQuint;

/**
 * Listener for {@link IQuintSource}s. It will get called whenever a quint is
 * produced, or changes happen to the source's state
 * 
 * @author Bastian
 * 
 */
public interface IQuintSourceListener {

	/**
	 * A that was pushed out of a source
	 * @param quint The quint pushed
	 */
	void pushedQuint(IQuint quint);

	/**
	 * Signals that the source has been closed and won't produce any new quints
	 */
	void sourceClosed();

	/**
	 * Signals that the source has been started and will produce quints afterwards
	 */
	void sourceStarted();

}
