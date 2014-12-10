package net.meisen.dissertation.impl.cache.hibernate;

import java.util.HashMap;
import java.util.Map;

import net.meisen.dissertation.config.xslt.DefaultValues;
import net.meisen.dissertation.exceptions.GeneralException;
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

public abstract class HibernateSessionManager {
	private final static Logger LOG = LoggerFactory
			.getLogger(HibernateSessionManager.class);

	protected final static class SessionTransactionWrapper {
		private final Session session;
		private final Transaction transaction;
		private final int commitSize;

		private int statements;

		public SessionTransactionWrapper(final SessionFactory factory,
				final int commitSize) {
			this.session = factory.openSession();
			this.transaction = session.beginTransaction();

			this.commitSize = commitSize;
			this.statements = 0;
		}

		public void statementHandled() {
			statements++;

			if (commitSize < 1) {
				close();
			} else if (statements % commitSize == 0) {
				this.transaction.commit();
			}
		}

		public Transaction getTransaction() {
			return transaction;
		}

		public Session getSession() {
			return session;
		}

		public void close() {
			this.transaction.commit();
			this.session.close();
		}

		@Override
		public String toString() {
			return session.toString() + " (" + commitSize + ")";
		}
	}

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

	public HibernateSessionManager() {
		this.initialized = false;
		this.entityName = null;

		this.persistency = true;
		this.commitSize = -1;

		this.currentWrapper = null;
	}

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

	public void setExceptionRegistry(final IExceptionRegistry exceptionRegistry) {
		this.exceptionRegistry = exceptionRegistry;
	}

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

	protected SessionFactory createUnmappedFactory() {
		final Map<String, String> override = new HashMap<String, String>();
		override.put(HibernateConfig.PROP_MAXPOOLSIZE, "1");

		final HibernateConfig config = getConfig();

		// create a builder and apply the configuration
		final StandardServiceRegistryBuilder builder = new StandardServiceRegistryBuilder();
		config.applyToHibernateBuilder(override, builder);

		// create a factory using the config
		final Configuration hibernateConfig = config
				.createHibernateConfig(override);

		// define the mappings using the dialect and create a new factory
		return hibernateConfig.buildSessionFactory(builder.build());
	}

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

	public String quote(final String name) {
		final Dialect dialect = getDialect();
		return dialect == null ? "[" + Strings.smartTrimSequence(name, "[")
				+ "]" : dialect.quote("`" + name + "`");
	}

	protected void registerException(final DefaultExceptionRegistry defReg,
			final Class<? extends RuntimeException> exception) {
		if (!defReg.isRegistered(exception)) {
			defReg.addExceptionCatalogByClass(exception,
					DefaultLocalizedExceptionCatalog.class);
		}
	}

	public String getEntityName() throws HibernateSessionManagerException {
		if (entityName == null) {
			exceptionRegistry.throwException(
					HibernateSessionManagerException.class, 1002);
		}

		return entityName;
	}

	public synchronized boolean setPersistency(final boolean enable)
			throws HibernateSessionManagerException {

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

	protected void remove() {
		if (this.initialized) {
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
			if (LOG.isErrorEnabled()) {
				LOG.error("Unable to cleanUp.", e);
			}
		}

		// close the factory again
		fac.close();
	}

	public int size() {
		final SessionTransactionWrapper wrapper = w();
		final int res = Numbers.castToInt((Long) wrapper.session
				.createCriteria(entityName)
				.setProjection(Projections.rowCount()).uniqueResult());
		wrapper.statementHandled();

		return res;
	}

	protected Dialect getDialect() {
		return dialect;
	}

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

	protected abstract String createEntityName(final TidaModel model);

	protected abstract Class<? extends RuntimeException>[] getExceptions();

	protected abstract HibernateConfig getConfig();

	protected abstract void defineMappings(final Configuration config,
			final Dialect dialect);
}
