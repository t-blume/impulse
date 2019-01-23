package main.java.common.implementation;

import main.java.common.interfaces.IQuint;
import main.java.common.interfaces.IResource;
import main.java.common.interfaces.ITimestamp;

/**
 * Implementation of a Quad, a statement made of subject,predicate,object and
 * context (where it came from)
 * 
 * @author Bastian
 *
 */
public class Quad implements IQuint {

	private IResource subject;
	private IResource predicate;
	private IResource object;
	private IResource context;

	private ITimestamp time;
	private int hash;

	/**
	 * Constructor without context. Warning: Context will be initialized by null
	 * 
	 * @param subject
	 *            The subject
	 * @param predicate
	 *            The predicate
	 * @param object
	 *            The object
	 */
	public Quad(IResource subject, IResource predicate, IResource object) {
		this(subject, predicate, object, null);
	}

	/**
	 * Constructor for a full quad
	 * 
	 * @param subject
	 *            The subject
	 * @param predicate
	 *            The predicate
	 * @param object
	 *            The object
	 * @param context
	 *            The context
	 */
	public Quad(IResource subject, IResource predicate, IResource object,
                IResource context) {
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
		this.context = context;

		// precompute hash, since quad is immutable
		hash = 17;
		hash = 31 * hash + this.subject.hashCode();
		hash = 31 * hash + this.predicate.hashCode();
		hash = 31 * hash + this.object.hashCode();

		if (this.context == null) {

			hash = 31 * hash;
		} else {
			hash = 31 * hash + this.context.hashCode();
		}

		this.time = new DateTimestamp();
	}

	@Override
	public IResource getSubject() {
		return subject;
	}

	@Override
	public IResource getPredicate() {
		return predicate;
	}

	@Override
	public IResource getObject() {
		return object;
	}

	@Override
	public IResource getContext() {
		return context;
	}

	@Override
	public ITimestamp getTimestamp() {
		return time;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Quad)) {
			return false;
		}
		Quad other = (Quad) obj;

		return subject.equals(other.subject)
				&& predicate.equals(other.predicate)
				&& object.equals(other.object)
				&& ((context == null && other.context == null) || context.equals(other.context));
				//context == null && other.conect != null -> nullpointer exception
	}

	@Override
	public int hashCode() {
		return hash;
	}

	@Override
	public String toString() {

		return "Subject: " + subject + ", Predicate: " + predicate
				+ ", Object: " + object + ", Context: " + context;
	}
}
