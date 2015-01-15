package net.meisen.dissertation.impl.cache.hibernate;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.meisen.dissertation.config.xslt.DefaultValues;
import net.meisen.dissertation.exceptions.GeneralException;
import net.meisen.dissertation.model.cache.ICache;
import net.meisen.dissertation.model.data.TidaModel;
import net.meisen.general.genmisc.exceptions.ForwardedRuntimeException;
import net.meisen.general.genmisc.exceptions.catalog.DefaultLocalizedExceptionCatalog;
import net.meisen.general.genmisc.exceptions.registry.DefaultExceptionRegistry;
import net.meisen.general.genmisc.exceptions.registry.IExceptionRegistry;
import net.meisen.general.genmisc.types.Numbers;
import net.meisen.general.genmisc.types.Strings;

import org.hibernate.JDBCException;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.Transaction;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.criterion.Projections;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * A base implementation for caches using {@code Hibernate}.
 * 
 * @author pmeisen
 * 
 * @param <T>
 *            the type of the identifier used to manage the enitites
 */
public abstract class HibernateSessionManager<T extends Serializable>
		implements ICache {
	private final static Logger LOG = LoggerFactory
			.getLogger(HibernateSessionManager.class);

	/**
	 * A wrapper for a session and transaction. The wrapper is used to bundle
	 * the commits and closes for the {@code Session} and the
	 * {@code Transaction}.
	 * 
	 * @author pmeisen
	 * 
	 * @see Session
	 * @see Transaction
	 * 
	 */
	protected final static class SessionTransactionWrapper {
		private final Session session;
		private final Transaction transaction;
		private final int commitSize;

		private int statements;

		/**
		 * Constructor specifying the factory and the commitSize of {@code this}
		 * .
		 * 
		 * @param factory
		 *            the {@code SessionFactory} of {@code Hibernate}
		 * @param commitSize
		 *            the amount of statements fired whenever changes should be
		 *            auto-commit; a negative commit-size indicates that only
		 *            one statement should be handled
		 */
		public SessionTransactionWrapper(final SessionFactory factory,
				final int commitSize) {
			this.session = factory.openSession();
			this.transaction = session.beginTransaction();

			this.commitSize = commitSize;
			this.statements = 0;
		}

		/**
		 * Inform the instance that a statement was handled. If a negative
		 * commit-size is specified, the session and transaction is closed,
		 * otherwise a commit is sent if the amount of statements reaches the
		 * commit-size.
		 */
		public void statementHandled() {
			statements++;

			if (commitSize < 1) {
				close();
			} else if (statements % commitSize == 0) {
				this.transaction.commit();
			}
		}

		/**
		 * Gets the transaction of {@code this}.
		 * 
		 * @return the transaction of {@code this}
		 */
		public Transaction getTransaction() {
			return transaction;
		}

		/**
		 * Gets the session of {@code this}.
		 * 
		 * @return the session of {@code this}
		 */
		public Session getSession() {
			return session;
		}

		/**
		 * Closes {@code this} and all resources, i.e. {@code Session} and
		 * {@code Transaction}.
		 */
		public void close() {
			this.transaction.commit();
			this.session.close();
		}

		@Override
		public String toString() {
			return session.toString() + " (" + commitSize + ")";
		}
	}

	/**
	 * The {@code ExceptionRegistry} for the manager.
	 */
	@Autowired
	@Qualifier(DefaultValues.EXCEPTIONREGISTRY_ID)
	protected IExceptionRegistry exceptionRegistry;

	private boolean initialized;
	private String entityName;
	private int commitSize;

	private boolean persistency;

	private SessionFactory factory;
	private Dialect dialect;

	private SessionTransactionWrapper currentWrapper;

	/**
	 * Default constructor.
	 */
	public HibernateSessionManager() {
		this.initialized = false;
		this.entityName = null;

		this.persistency = true;
		this.commitSize = -1;

		this.currentWrapper = null;
	}

	@Override
	public void initialize(final TidaModel model)
			throws HibernateSessionManagerException {

		if (initialized) {
			exceptionRegistry.throwException(
					HibernateSessionManagerException.class, 1001);
		}
		// first of all make sure the exceptions are registered
		else if (exceptionRegistry instanceof DefaultExceptionRegistry) {
			final DefaultExceptionRegistry defReg = (DefaultExceptionRegistry) exceptionRegistry;

			// add the default exceptions
			registerException(defReg, HibernateSessionManagerException.class);

			// add additional exceptions
			final Class<? extends RuntimeException>[] exceptions = getExceptions();
			if (exceptions != null) {
				for (final Class<? extends RuntimeException> exception : exceptions) {
					registerException(defReg, exception);
				}
			}
		} else {
			throw new ForwardedRuntimeException(GeneralException.class, 1002,
					getClass().getSimpleName());
		}

		// validate the configuration
		if (getConfig() == null) {
			exceptionRegistry.throwException(
					HibernateSessionManagerException.class, 1003);
		}

		// get the name of the entity
		this.entityName = createEntityName(model);
		this.dialect = determineDialect();

		// create the factory
		this.factory = createFactory();

		// log the init
		if (LOG.isTraceEnabled()) {
			final String className = this.getClass().getSimpleName();
			LOG.trace("Initialized '"
					+ (className == null || className.isEmpty() ? HibernateSessionManager.class
							.getSimpleName() : className) + "' of entity '"
					+ getEntityName() + "'.");
		}

		this.initialized = true;
	}

	/**
	 * Specifies the {@code ExceptionRegistry} to be used. Might be auto-wired.
	 * 
	 * @param exceptionRegistry
	 *            the registry to be used
	 */
	public void setExceptionRegistry(final IExceptionRegistry exceptionRegistry) {
		this.exceptionRegistry = exceptionRegistry;
	}

	/**
	 * Determines the dialect of the underlying database used.
	 * 
	 * @return the used {@code Dialect} or {@code null} if no dialect can be
	 *         determined
	 */
	protected Dialect determineDialect() {

		// define the mappings using the dialect and create a new factory
		final SessionFactory fac = createUnmappedFactory();

		final Dialect dialect;
		if (fac instanceof SessionFactoryImplementor) {
			dialect = ((SessionFactoryImplementor) fac).getDialect();
		} else {
			dialect = null;
		}
		fac.close();

		return dialect;
	}

	/**
	 * Creates an unmapped factory. The factory is mainly used to fire a single
	 * statement or to determine settings. It can not be used to manipulate or
	 * work with the entities.
	 * 
	 * @return an unmapped factory
	 * 
	 * @throws HibernateSessionManagerException
	 *             if the factory cannot be created
	 */
	protected SessionFactory createUnmappedFactory()
			throws HibernateSessionManagerException {
		final Map<String, String> override = new HashMap<String, String>();
		override.put(HibernateConfig.PROP_MAXPOOLSIZE, "1");

		final HibernateConfig config = getConfig();

		try {
			// create a builder and apply the configuration
			final StandardServiceRegistryBuilder builder = new StandardServiceRegistryBuilder();
			config.applyToHibernateBuilder(override, builder);

			// create a factory using the config
			final Configuration hibernateConfig = config
					.createHibernateConfig(override);

			// define the mappings using the dialect and create a new factory
			return hibernateConfig.buildSessionFactory(builder.build());
		} catch (final Throwable t) {

			// catch any exception
			exceptionRegistry.throwException(
					HibernateSessionManagerException.class, 1000, t, config,
					t.getMessage());
			return null;
		}
	}

	/**
	 * Creates a factory useful to work with the entities.
	 * 
	 * @return the created factory
	 * 
	 * @throws HibernateSessionManagerException
	 *             if the factory cannot be created
	 */
	protected SessionFactory createFactory()
			throws HibernateSessionManagerException {
		final HibernateConfig config = getConfig();

		// set some values for the manager
		this.commitSize = config.getCommitSize();

		// create the factory
		try {

			// create a builder and apply the configuration
			final StandardServiceRegistryBuilder builder = new StandardServiceRegistryBuilder();
			config.applyToHibernateBuilder(builder);

			// create a factory using the config
			final Configuration hibernateConfig = config
					.createHibernateConfig();

			// define the mappings using the dialect and create a new factory
			defineMappings(hibernateConfig, dialect);
			return hibernateConfig.buildSessionFactory(builder.build());
		} catch (final Throwable t) {

			// catch any exception
			exceptionRegistry.throwException(
					HibernateSessionManagerException.class, 1000, t, config,
					t.getMessage());
			return null;
		}
	}

	/**
	 * Quotes the {@code name} using the {@code Dialect} of {@code this}.
	 * 
	 * @param name
	 *            the name to be quoted
	 * 
	 * @return the quoted name
	 */
	public String quote(final String name) {
		final Dialect dialect = getDialect();
		return dialect == null ? "[" + Strings.smartTrimSequence(name, "[")
				+ "]" : dialect.quote("`" + name + "`");
	}

	/**
	 * Register the {@code exception} in {@code defReg}.
	 * 
	 * @param defReg
	 *            the registry to register the exception at
	 * @param exception
	 *            the exception-type to be added
	 */
	protected void registerException(final DefaultExceptionRegistry defReg,
			final Class<? extends RuntimeException> exception) {
		if (!defReg.isRegistered(exception)) {
			defReg.addExceptionCatalogByClass(exception,
					DefaultLocalizedExceptionCatalog.class);
		}
	}

	/**
	 * Gets the name of the entity.
	 * 
	 * @return the name of the entity
	 * 
	 * @throws HibernateSessionManagerException
	 *             if the name is invalid
	 */
	public String getEntityName() throws HibernateSessionManagerException {
		if (entityName == null) {
			exceptionRegistry.throwException(
					HibernateSessionManagerException.class, 1002);
		}

		return entityName;
	}

	@Override
	public synchronized boolean setPersistency(final boolean enable) {

		/*
		 * Make sure that there is no other thread working with the cache
		 * currently.
		 */
		final boolean oldPersistency = this.persistency;
		this.persistency = enable;

		// if not initialized we are done here
		if (!initialized) {
			this.persistency = enable;
			return oldPersistency;
		}

		// nothing to do, nothing was changed
		if (oldPersistency == this.persistency) {
			// nothing
		}
		/*
		 * Persistency was enabled and is disabled now.
		 */
		else if (oldPersistency) {
			// nothing to do
		}
		/*
		 * Persistency was disabled and is enabled now, write the not persisted
		 * once.
		 */
		else {
			if (this.currentWrapper != null) {

				// reset the wrapper
				this.currentWrapper.close();
				this.currentWrapper = null;
			}
		}

		return oldPersistency;
	}

	@Override
	public synchronized void release() {
		if (!this.initialized) {
			return;
		}

		// make sure everything is persisted and closed
		setPersistency(true);
		if (this.currentWrapper != null) {
			throw new IllegalStateException(
					"The currentWrapper should never be not null here.");
		}

		// close the factory
		this.factory.close();
		this.initialized = false;

		// log the closing
		if (LOG.isTraceEnabled()) {
			final String className = this.getClass().getSimpleName();
			LOG.trace("Closed '"
					+ (className == null || className.isEmpty() ? HibernateSessionManager.class
							.getSimpleName() : className) + "' of entity '"
					+ getEntityName() + "'.");
		}
	}

	@Override
	public void remove() {
		final boolean init = this.initialized;

		if (init) {
			release();
		}
		// if there is no entity there is nothing to be deleted
		else if (entityName == null) {
			return;
		}

		// create a new factory, because the one used might be closed
		final SessionFactory fac = createUnmappedFactory();

		// open the session and drop the table
		try {
			final StatelessSession session = fac.openStatelessSession();
			final SQLQuery query = session.createSQLQuery("DROP TABLE "
					+ quote(entityName));
			query.executeUpdate();
			session.close();
		} catch (final JDBCException e) {
			if (init) {
				if (LOG.isErrorEnabled()) {
					LOG.error("Unable to cleanUp.", e);
				}
			} else if (LOG.isTraceEnabled()) {
				if (LOG.isTraceEnabled()) {
					LOG.trace("Unable to cleanUp.", e);
				}
			}
		}

		// close the factory again
		fac.close();
	}

	/**
	 * Gets the amount of the cached entities.
	 * 
	 * @return the amount of the cached entities
	 */
	public int size() {
		final SessionTransactionWrapper wrapper = w();
		final int res = Numbers.castToInt((Long) wrapper.session
				.createCriteria(entityName)
				.setProjection(Projections.rowCount()).uniqueResult());
		wrapper.statementHandled();

		return res;
	}

	/**
	 * Gets the dialect of {@code this}.
	 * 
	 * @return the dialect of {@code this}
	 * 
	 * @see Dialect
	 */
	protected Dialect getDialect() {
		return dialect;
	}

	/**
	 * Gets the current {@code SessionTransactionWrapper} or creates a new one.
	 * 
	 * @return the current {@code SessionTransactionWrapper} or creates a new
	 *         one
	 * @throws HibernateSessionManagerException
	 *             if the manager isn't initialized
	 */
	protected SessionTransactionWrapper w()
			throws HibernateSessionManagerException {
		if (!initialized) {
			exceptionRegistry.throwException(
					HibernateSessionManagerException.class, 1002);
			return null;
		} else if (this.currentWrapper == null) {

			if (this.persistency) {

				// create a new one
				return new SessionTransactionWrapper(factory, -1);
			} else {

				// create a new session and keep it
				this.currentWrapper = new SessionTransactionWrapper(factory,
						commitSize);
				return this.currentWrapper;
			}
		} else {
			return this.currentWrapper;
		}
	}

	/**
	 * Creates the name of the entity for the concrete implementation and the
	 * model the specified model. The value should be retrieved using
	 * {@link #getEntityName()}.
	 * 
	 * @param model
	 *            the model to create the {@code entityName} for
	 * 
	 * @return the created name
	 */
	protected abstract String createEntityName(final TidaModel model);

	/**
	 * Gets the exceptions which should be registered for the concrete
	 * implementation.
	 * 
	 * @return the exceptions which should be registered for the concrete
	 *         implementation
	 */
	protected abstract Class<? extends RuntimeException>[] getExceptions();

	/**
	 * Gets the configuration for {@code this}.
	 * 
	 * @return the configuration for {@code this}
	 */
	protected abstract HibernateConfig getConfig();

	/**
	 * Define the mapping of the concrete implementation.
	 * 
	 * @param config
	 *            the {@code Configuration} of {@code this}
	 * @param dialect
	 *            the dialect
	 */
	protected abstract void defineMappings(final Configuration config,
			final Dialect dialect);

	/**
	 * Saves the specified map using the specified {@code id}. If the identifier
	 * is {@code null} the map will be inserted for sure, otherwise it will be
	 * checked and updated.
	 * 
	 * @param map
	 *            the record to be saved
	 * @param id
	 *            the id of the record
	 */
	protected synchronized void saveMap(final Map<String, Object> map,
			final T id) {
		final String entityName = getEntityName();
		final SessionTransactionWrapper wrapper = w();

		// get the value
		final boolean update;
		if (id == null) {
			update = false;
		} else {
			update = wrapper.getSession().get(entityName, id) != null;
		}

		// check if update has to be performed
		if (update) {
			wrapper.getSession().merge(entityName, map);
		} else {
			wrapper.getSession().save(entityName, map);
		}

		wrapper.statementHandled();
	}

	/**
	 * Gets the map for the specified {@code id}.
	 * 
	 * @param id
	 *            the identifier to be retrieved
	 * 
	 * @return the record as map or {@code null} if none exists
	 */
	@SuppressWarnings("unchecked")
	protected final Map<String, Object> getMap(final T id) {
		final String entityName = getEntityName();
		final SessionTransactionWrapper wrapper = w();

		final Object value = wrapper.getSession().get(entityName, id);
		wrapper.statementHandled();

		if (value instanceof Map) {
			return (Map<String, Object>) value;
		} else {
			return null;
		}
	}

	/**
	 * Creates an iterator for the identifiers of the entities.
	 * 
	 * @return an iterator for the identifiers of the entities
	 */
	protected Iterator<T> createIterator() {
		final SessionTransactionWrapper wrapper = w();

		@SuppressWarnings("unchecked")
		final List<T> list = wrapper.getSession()
				.createQuery("SELECT id FROM " + getEntityName()).list();
		wrapper.statementHandled();

		return list.iterator();
	}
}
