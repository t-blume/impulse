package main.java.common.implementation;

import main.java.common.interfaces.IResource;

import java.io.Serializable;

/**
 * A wrapper resource, which enriches a given resource with a type. For example
 * to distinguish different contexts for the same identifier
 * 
 * @author Bastian
 *
 */
public class TypedResource implements IResource, Serializable {

	private String type;

	private IResource resource;

	/**
	 * Constructor
	 * 
	 * @param res
	 *            The resource to be encapsulated
	 * @param type
	 *            The type given to the resource
	 */
	public TypedResource(IResource res, String type) {
		this.type = type;
		resource = res;
	}

	public String getType() {
		return type;
	}

	public IResource getResource() {
		return resource;
	}

	@Override
	public String toString() {
		return resource.toString();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof TypedResource)) {
			return false;
		}

		TypedResource other = (TypedResource) obj;

		return type.equals(other.type) && resource.equals(other.resource);
	}

	@Override
	public int hashCode() {
		return 17 + type.hashCode() + 31 * resource.hashCode();
	}

	@Override
	public String toN3() {
		return resource.toN3();
	}

}
