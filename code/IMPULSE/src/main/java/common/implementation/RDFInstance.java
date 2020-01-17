package main.java.common.implementation;

import main.java.common.interfaces.IInstanceElement;
import main.java.common.interfaces.IQuint;
import org.bson.Document;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * An instance which encapsulates a multitude of statements made about it
 *
 * @author Bastian
 *
 */
public class RDFInstance implements IInstanceElement, Serializable {

	public static final String LOCATOR_KEY = "locator";
	private static final String OUTGOING_KEY = "outgoing";
	private static final String INCOMING_KEY = "incoming";

	private Set<IQuint> outgoingQuints;
	private Set<IQuint> incomingQuints;

	private Integer locator;


	/**
	 * Constructs a new instance, which can be referenced by the given locator
	 *
	 * @param locator The locator for this instance
	 */
	public RDFInstance(int locator) {
		this.locator = locator;
		outgoingQuints = new HashSet<>();
		incomingQuints = new HashSet<>();
	}
	public RDFInstance(Document document){
		this.locator = (Integer) document.get(LOCATOR_KEY);
		ArrayList<Document> outgoing = (ArrayList<Document>) document.get(OUTGOING_KEY);
		this.outgoingQuints = new HashSet<>();
		outgoing.forEach(d -> outgoingQuints.add(new Quad(d)));
		ArrayList<Document> incoming = (ArrayList<Document>) document.get(INCOMING_KEY);
		this.incomingQuints =  new HashSet<>();
		incoming.forEach(d -> incomingQuints.add(new Quad(d)));
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
		IInstanceElement element = new RDFInstance(locator);
		for(IQuint quint : getOutgoingQuints())
			element.addOutgoingQuint(quint);
		for(IQuint quint : getIncomingQuints())
			element.addIncomingQuint(quint);
		return element;
	}

	@Override
	public int getLocator() {
		return locator;
	}

	@Override
	public Document toDocument() {
		Document document = new Document();
		document.put(LOCATOR_KEY, locator);
		ArrayList<Document> outgoing = new ArrayList<>();
		outgoingQuints.forEach(q -> outgoing.add(q.toDocument()));
		document.put(OUTGOING_KEY, outgoing);
		ArrayList<Document> incoming = new ArrayList<>();
		incomingQuints.forEach(q -> incoming.add(q.toDocument()));
		document.put(INCOMING_KEY, incoming);
		return document;
	}


	@Override
	public String toString() {
		return "Instance: " + locator + "\n" + "\tOutgoing: " + outgoingQuints.size()
				+ " Incoming: " + incomingQuints.size();
	}

}
