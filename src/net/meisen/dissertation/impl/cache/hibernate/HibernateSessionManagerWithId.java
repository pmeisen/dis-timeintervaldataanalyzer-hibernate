package net.meisen.dissertation.impl.cache.hibernate;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class HibernateSessionManagerWithId<T extends Serializable>
		extends HibernateSessionManager {
	private final static Logger LOG = LoggerFactory
			.getLogger(HibernateSessionManagerWithId.class);

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

	protected Iterator<T> createIterator() {
		final SessionTransactionWrapper wrapper = w();

		@SuppressWarnings("unchecked")
		final List<T> list = wrapper.getSession()
				.createQuery("SELECT id FROM " + getEntityName()).list();
		wrapper.statementHandled();
		
		return list.iterator();
	}
}
