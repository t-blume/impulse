package main.java.processing.implementation.preprocessing;


import main.java.common.implementation.RDFInstance;
import main.java.common.interfaces.IInstanceElement;
import main.java.common.interfaces.IQuint;
import main.java.input.interfaces.IQuintListener;
import main.java.processing.interfaces.IElementCache;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Aggregates information of instances
 * 
 * @author Bastian
 * @editor Till
 */
public class InstanceAggregator implements IQuintListener {
	private static final Logger logger = LogManager.getLogger(InstanceAggregator.class.getSimpleName());
	
	private IElementCache<IInstanceElement> window;

	/**
	 *
	 * @param window
	 */
	public InstanceAggregator(IElementCache<IInstanceElement> window) {
		this.window = window;
	}

	@Override
	public void finishedQuint(IQuint i) {
		addQuint2Cache(i, true);
	}

	protected IInstanceElement createInstance(IQuint quint){
		return new RDFInstance(quint.getSubject());
	}

	protected void addQuint2Cache(IQuint quint, boolean asOutgoing){
		IInstanceElement element = createInstance(quint);
		if (window.contains(element.getLocator()))
			element = window.get(element.getLocator());
		else
			window.add(element);

		if(asOutgoing)
			element.addOutgoingQuint(quint);
		else
			element.addIncomingQuint(quint);
	}

	@Override
	public void finished() {
		logger.debug("Finished aggregating");
		window.close();
	}
}
