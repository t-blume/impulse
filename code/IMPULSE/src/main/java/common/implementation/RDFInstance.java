package main.java.common.implementation;

import main.java.common.interfaces.IInstanceElement;
import main.java.common.interfaces.IQuint;
import main.java.common.interfaces.IResource;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * An instance which encapsulates a multitude of statements made about it
 *
 * @author Bastian
 *
 */
public class RDFInstance implements IInstanceElement, Serializable {

	private Set<IQuint> outgoingQuints;
	private Set<IQuint> incomingQuints;

	private IResource resource;

	public static String RESOURCE_TYPE = "Instance";

	/**
	 * Constructs a new instance, which can be referenced by the given locator
	 *
	 * @param locator The locator for this instance
	 */
	public RDFInstance(IResource locator) {
		this.resource = new TypedResource(locator, RESOURCE_TYPE);
		outgoingQuints = new HashSet<>();
		incomingQuints = new HashSet<>();
	}

	@Override
	public Set<IQuint> getOutgoingQuints() {
		return outgoingQuints;
	}
	@Override
	public Set<IQuint> getIncomingQuints() {
		return incomingQuints;
	}

	@Override
	public void addOutgoingQuint(IQuint q) {
		outgoingQuints.add(q);
	}
	@Override
	public void addIncomingQuint(IQuint q) {
		incomingQuints.add(q);
	}

	@Override
	public IInstanceElement clone() {
		IInstanceElement element = new RDFInstance(getLocator());
		for(IQuint quint : getOutgoingQuints())
			element.addOutgoingQuint(quint);
		for(IQuint quint : getIncomingQuints())
			element.addIncomingQuint(quint);
		return element;
	}

	@Override
	public String toString() {
		return "Instance: " + resource + "\n" + "\tOutgoing: " + outgoingQuints.size()
				+ " Incoming: " + incomingQuints.size();
	}

	@Override
	public IResource getLocator() {
		return resource;
	}

}
