package main.java.common.implementation;

import main.java.common.interfaces.IQuint;
import org.bson.Document;

import java.io.Serializable;

/**
 * Implementation of a Quad, a statement made of subject,predicate,object and
 * context (where it came from)
 * 
 * @author Bastian
 *
 */
public class Quad implements IQuint, Serializable {
	private static final String SUBJECT_KEY = "subject";
	private static final String PREDICATE_KEY = "predicate";
	private static final String OBJECT_KEY = "object";
	private static final String CONTEXT_KEY = "context";


	private String subject;
	private String predicate;
	private String object;
	private String context;

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
	public Quad(String subject, String predicate, String object) {
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
	public Quad(String subject, String predicate, String object,
				String context) {
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
	}

	public Quad(Document document) {
		this.subject = document.getString(SUBJECT_KEY);
		this.predicate = document.getString(PREDICATE_KEY);
		this.object = document.getString(OBJECT_KEY);
		this.context = document.getString(CONTEXT_KEY);

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
	}

	@Override
	public String getSubject() {
		return subject;
	}

	@Override
	public String getPredicate() {
		return predicate;
	}

	@Override
	public String getObject() {
		return object;
	}

	@Override
	public String getContext() {
		return context;
	}

	@Override
	public String getTimestamp() {
		return null;
	}

	@Override
	public Document toDocument() {
		Document document = new Document();
		document.put(SUBJECT_KEY, subject);
		document.put(PREDICATE_KEY, predicate);
		document.put(OBJECT_KEY, object);
		document.put(CONTEXT_KEY, context);
		return document;
		//return  "{ \"subject\": \"" + subject + "\", \"predicate\": \"" + predicate
		//		+ "\", \"object\": \"" + object + "\", \"context\": \"" + context + "\"}";
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
