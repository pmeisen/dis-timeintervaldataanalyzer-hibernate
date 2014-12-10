package net.meisen.dissertation.impl.cache.hibernate;

/**
 * Exception thrown when a problem with the {@code HibernateBitmapCache} occurs.
 * 
 * @author pmeisen
 * 
 */
public class HibernateFactDescriptorModelSetCacheException extends RuntimeException {
	private static final long serialVersionUID = -4632985916064084663L;

	/**
	 * Creates an exception which should been thrown whenever there is no other
	 * reason for the exception, i.e. the exception is the root.
	 * 
	 * @param message
	 *            the message of the exception
	 */
	public HibernateFactDescriptorModelSetCacheException(final String message) {
		super(message);
	}

	/**
	 * Creates an exception which should been thrown whenever another
	 * <code>Throwable</code> is the reason for this.
	 * 
	 * @param message
	 *            the message of the exception
	 * @param t
	 *            the reason for the exception
	 */
	public HibernateFactDescriptorModelSetCacheException(final String message, final Throwable t) {
		super(message, t);
	}
}
