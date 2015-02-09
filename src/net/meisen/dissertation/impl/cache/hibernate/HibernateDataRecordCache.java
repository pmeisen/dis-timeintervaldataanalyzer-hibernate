package net.meisen.dissertation.impl.cache.hibernate;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import net.meisen.dissertation.impl.cache.BaseIdentifierCacheException;
import net.meisen.dissertation.jdbc.protocol.DataType;
import net.meisen.dissertation.model.cache.IDataRecordCache;
import net.meisen.dissertation.model.cache.IDataRecordCacheConfig;
import net.meisen.dissertation.model.data.TidaModel;
import net.meisen.dissertation.model.indexes.datarecord.IDataRecordMeta;
import net.meisen.dissertation.model.indexes.datarecord.ProcessedDataRecord;
import net.meisen.dissertation.model.time.mapper.BaseMapper;
import net.meisen.dissertation.model.util.IIntIterator;

import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Mappings;
import org.hibernate.dialect.Dialect;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.PrimaryKey;
import org.hibernate.mapping.Property;
import org.hibernate.mapping.RootClass;
import org.hibernate.mapping.SimpleValue;
import org.hibernate.mapping.Table;

/**
 * A concrete implementation of a {@code DataRecordCache} using hibernate.
 * 
 * @author pmeisen
 * 
 * @see IDataRecordCache
 * 
 */
public class HibernateDataRecordCache extends HibernateSessionManager<Integer>
		implements IDataRecordCache {

	private BaseMapper<?> mapper;
	private IDataRecordMeta meta;

	private HibernateDataRecordCacheConfig config;

	/**
	 * Default constructor.
	 */
	public HibernateDataRecordCache() {
		this.config = null;
	}

	@Override
	public void initialize(final TidaModel model) {
		
		// get the needed values
		this.mapper = model.getIntervalModel().getTimelineMapper();
		this.meta = model.getDataRecordFactory().getMeta();

		// now initialize
		super.initialize(model);
	}

	protected String createEntityName(final TidaModel model) {
		return "records_" + model.getId();
	}

	@Override
	protected Class<? extends RuntimeException>[] getExceptions() {
		@SuppressWarnings("unchecked")
		final Class<? extends RuntimeException>[] exp = new Class[] { HibernateDataRecordCacheException.class };

		return exp;
	}

	@Override
	protected void defineMappings(final Configuration config,
			final Dialect dialect) {
		final String entityName = getEntityName();

		final Mappings mappings = config.createMappings();
		final Table table = mappings.addTable(null, null, entityName, null,
				false);

		final RootClass clazz = new RootClass();
		clazz.setEntityName(entityName);
		clazz.setJpaEntityName(entityName);
		clazz.setLazy(true);
		clazz.setTable(table);

		final String[] names = this.meta.getNames();
		final DataType[] types = this.meta.getDataTypes();
		for (int i = 0; i < names.length; i++) {
			final boolean isKey = (i == this.meta.getPosRecordId() - 1);

			// get the name
			final String name = names[i];
			final String quotedName = quote(name);

			// create the column
			final Column column = new Column();
			column.setName(quotedName);
			column.setNullable(!isKey);
			column.setSqlTypeCode(types[i].getSqlType());
			table.addColumn(column);

			final Property p = new Property();
			p.setName(names[i]);
			clazz.addProperty(p);

			final SimpleValue v = new SimpleValue(mappings);
			v.setTable(table);
			v.setTypeName(types[i].getRepresentorClass().getName());
			v.addColumn(column);
			p.setValue(v);

			// add the key if it's the primary one
			if (isKey) {

				// create the key
				final PrimaryKey primaryKey = new PrimaryKey();
				primaryKey.setName("PK_" + entityName);
				primaryKey.setTable(table);
				primaryKey.addColumn(column);

				// mark the strategy for the key
				v.setIdentifierGeneratorStrategy(SimpleValue.DEFAULT_ID_GEN_STRATEGY);

				// add the key to the table
				table.setPrimaryKey(primaryKey);
				table.setIdentifierValue(v);

				// specify the idenitifer property for the class
				clazz.setIdentifier(v);
				clazz.setIdentifierProperty(p);

				// mark the property
				p.setInsertable(false);
				p.setUpdateable(false);
			}
		}

		mappings.addClass(clazz);
	}

	/**
	 * Gets the used {@code DataTypes} of the different values of the record.
	 * 
	 * @return the used {@code DataTypes} of the different values of the record
	 */
	public DataType[] getDataTypes() {
		return meta.getDataTypes();
	}

	@Override
	public void cache(final ProcessedDataRecord record) {
		final Object[] res = record.createObjectArray(meta, mapper);
		cache(record.getId(), res);
	}

	@Override
	public void cache(final int id, final Object[] record) {

		// create the map
		final Map<String, Object> map = new HashMap<String, Object>(
				record.length);
		final String[] names = this.meta.getNames();
		for (int i = 0; i < names.length; i++) {
			map.put(names[i], record[i]);
		}

		cache(map);
	}

	/**
	 * Caches/Persists the specified map.
	 * 
	 * @param map
	 *            the map to be cached/persisted
	 */
	public void cache(final Map<String, Object> map) {
		// it's always an insert
		saveMap(map, null);
	}

	@Override
	public void release() {
		super.release();
	}

	@Override
	public Object[] get(final int recordId) {
		final Map<String, Object> map = getMap(recordId);

		if (map == null) {
			return null;
		} else {
			final String[] names = this.meta.getNames();

			// recreate the record
			final Object[] record = new Object[names.length];
			final DataType[] types = this.meta.getDataTypes();
			for (int i = 0; i < names.length; i++) {

				// date is probably a timestamp, which we don't want
				if (DataType.DATE.equals(types[i])) {
					final Date date = (Date) map.get(names[i]);
					record[i] = new Date(date.getTime());
				} else {
					record[i] = map.get(names[i]);
				}

			}

			return record;
		}
	}

	@Override
	public IIntIterator intIterator() {
		final Iterator<Integer> it = this.iterator();

		return new IIntIterator() {

			@Override
			public int next() {
				return it.next();
			}

			@Override
			public boolean hasNext() {
				return it.hasNext();
			}
		};
	}

	@Override
	public Iterator<Integer> iterator() {
		return createIterator();
	}

	@Override
	public int size() {
		return super.size();
	}

	@Override
	public void setConfig(final IDataRecordCacheConfig config)
			throws BaseIdentifierCacheException {
		if (config instanceof HibernateDataRecordCacheConfig) {
			this.config = (HibernateDataRecordCacheConfig) config;
		} else {

			// we have an invalid configuration
			this.config = null;
		}
	}

	@Override
	protected HibernateDataRecordCacheConfig getConfig() {
		return config;
	}
}
