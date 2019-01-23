package main.java.common.interfaces;

/**
 * This interface marks classes which provide locators, that is, they may be referenced by an {@link IResource
 * @author Bastian
 *
 */
public interface ILocatable {

	/**
	 * Provides an IResource by which the object may be referenced by
	 * @return The locator object
	 */
	IResource getLocator();
}
