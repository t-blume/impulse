package main.java.common.implementation;

import main.java.common.interfaces.IResource;
import org.semanticweb.yars.nx.Node;

import java.io.Serializable;


/**
 * A resource containing a {@link Node} provided by the NxParser framework
 * 
 * @author Bastian
 *
 */
public class NodeResource implements IResource, Serializable {
	private Node res;

	/**
	 * Constructor
	 * 
	 * @param resource
	 *            The internal resource
	 */
	public NodeResource(Node resource) {
		this.res = resource;
	}

	/**
	 * Getter for the raw internal {@link Node}
	 * 
	 * @return The internal node
	 */
	public Node getNode() {
		return res;
	}

	@Override
	public int hashCode() {
		return res.hashCode();
	}

	@Override
	public String toString() {
		return res.getLabel();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof NodeResource)) {
			return false;
		}

		NodeResource other = (NodeResource) obj;

		return res.equals(other.res);
	}

	@Override
	public String toN3() {
		return res.toString();
	}
}
