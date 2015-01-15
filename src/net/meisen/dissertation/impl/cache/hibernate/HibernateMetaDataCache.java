package net.meisen.dissertation.impl.cache.hibernate;

import java.sql.Types;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import net.meisen.dissertation.impl.data.metadata.DescriptorMetaDataCollection;
import net.meisen.dissertation.impl.data.metadata.LoadedMetaData;
import net.meisen.dissertation.jdbc.protocol.DataType;
import net.meisen.dissertation.model.cache.IMetaDataCache;
import net.meisen.dissertation.model.cache.IMetaDataCacheConfig;
import net.meisen.dissertation.model.data.TidaModel;
import net.meisen.dissertation.model.data.metadata.IMetaDataCollection;
import net.meisen.dissertation.model.descriptors.Descriptor;
import net.meisen.general.genmisc.types.Streams;

import org.hibernate.Query;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Mappings;
import org.hibernate.dialect.Dialect;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.PrimaryKey;
import org.hibernate.mapping.Property;
import org.hibernate.mapping.RootClass;
import org.hibernate.mapping.SimpleValue;
import org.hibernate.mapping.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code Hibernate} cache used for meta-data.
 * 
 * @author pmeisen
 * 
 */
public class HibernateMetaDataCache extends HibernateSessionManager<String>
		implements IMetaDataCache {
	private final static Logger LOG = LoggerFactory
			.getLogger(HibernateMetaDataCache.class);

	private HibernateMetaDataCacheConfig config;

	/**
	 * Default constructor.
	 */
	public HibernateMetaDataCache() {
		this.config = null;
	}

	@Override
	public void cacheDescriptor(final Descriptor<?, ?, ?> desc) {

		// persist the descriptor
		final Map<String, Object> record = new HashMap<String, Object>();

		final String modelId = desc.getModelId();
		final String descId = DatatypeConverter.printBase64Binary(Streams
				.objectToByte(desc.getId()));
		final byte[] value = Streams.objectToByte(desc.getValue());

		final String key = modelId + " " + descId;
		
		record.put("key", key);
		record.put("value", value);

		this.saveMap(record, key);
	}

	@Override
	public IMetaDataCollection createMetaDataCollection() {
		final DescriptorMetaDataCollection collection = new DescriptorMetaDataCollection();

		if (size() > 0) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Using database meta-data.");
			}

			final Query query = w().getSession().createQuery(
					"from " + getEntityName() + " order by key");
			final Iterator<?> it = query.iterate();

			LoadedMetaData curMetaData = null;
			while (it.hasNext()) {

				@SuppressWarnings("unchecked")
				final Map<String, Object> record = (Map<String, Object>) it
						.next();

				final String compId = (String) record.get("key");
				final byte[] byteValue = (byte[]) record.get("value");

				// split the key and get the values
				final String[] sepKeys = compId.split(" ");
				final String modelId = sepKeys[0];
				final byte[] byteDescId = DatatypeConverter
						.parseBase64Binary(sepKeys[1]);
				final Object descId = Streams.byteToObject(byteDescId).object;

				// get the final result
				final Object value = Streams.byteToObject(byteValue).object;

				// create the metadata instance
				if (curMetaData == null
						|| !curMetaData.getDescriptorModelId().equals(modelId)) {
					curMetaData = new LoadedMetaData(modelId);
					collection.addMetaData(curMetaData);
				}
				curMetaData.addValue(descId, value);
			}
		}

		return collection;
	}

	@Override
	public void setConfig(final IMetaDataCacheConfig config) {
		if (config instanceof HibernateMetaDataCacheConfig) {
			this.config = (HibernateMetaDataCacheConfig) config;
		} else {

			// we have an invalid configuration
			this.config = null;
		}
	}

	@Override
	protected String createEntityName(final TidaModel model) {
		return "metadata_" + model.getId();
	}

	@Override
	protected Class<? extends RuntimeException>[] getExceptions() {
		@SuppressWarnings("unchecked")
		final Class<? extends RuntimeException>[] exp = new Class[] { HibernateDataRecordCacheException.class };

		return exp;
	}

	@Override
	protected HibernateMetaDataCacheConfig getConfig() {
		return config;
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

		// create the key
		createKeyMapping(mappings, table, clazz, dialect);
		createAdditionalMappings(mappings, table, clazz, dialect);

		// add the class to the mapping
		mappings.addClass(clazz);
	}

	/**
	 * Method to create the mappings needed for an bitmap-identifier.
	 * 
	 * @param mappings
	 *            the {@code Mappings}
	 * @param table
	 *            the table defined
	 * @param clazz
	 *            the defined class
	 * @param dialect
	 *            the dialect of the database
	 */
	protected void createKeyMapping(final Mappings mappings, final Table table,
			final RootClass clazz, final Dialect dialect) {

		final Column column = new Column();
		column.setName(quote("key"));
		column.setNullable(false);
		column.setSqlTypeCode(DataType.STRING.getSqlType());
		column.setLength(1000);

		final SimpleValue v = new SimpleValue(mappings);
		v.setTable(table);
		v.setTypeName(String.class.getName());
		v.addColumn(column);
		v.setIdentifierGeneratorStrategy(SimpleValue.DEFAULT_ID_GEN_STRATEGY);

		final Property p = new Property();
		p.setName("key");
		p.setValue(v);
		p.setInsertable(false);
		p.setUpdateable(false);

		final PrimaryKey primaryKey = new PrimaryKey();
		primaryKey.setName("PK_" + getEntityName());
		primaryKey.setTable(table);
		primaryKey.addColumn(column);

		table.addColumn(column);
		table.setPrimaryKey(primaryKey);
		table.setIdentifierValue(v);

		clazz.addProperty(p);
		clazz.setIdentifier(v);
		clazz.setIdentifierProperty(p);
	}

	/**
	 * Method called to add mappings to the {@code Hibernate} definition.
	 * 
	 * @param mappings
	 *            the {@code Mappings}
	 * @param table
	 *            the table defined
	 * @param clazz
	 *            the defined class
	 * @param dialect
	 *            the dialect of the database
	 */
	protected void createAdditionalMappings(final Mappings mappings,
			final Table table, final RootClass clazz, final Dialect dialect) {

		// create the column for the bitmap
		final Column cValue = new Column();
		cValue.setName(quote("value"));
		cValue.setNullable(false);
		cValue.setLength(Integer.MAX_VALUE);
		cValue.setSqlTypeCode(Types.VARBINARY);

		final SimpleValue vValue = new SimpleValue(mappings);
		vValue.setTable(table);
		vValue.setTypeName(byte[].class.getName());
		vValue.addColumn(cValue);

		final Property pValue = new Property();
		pValue.setName("value");
		pValue.setLob(true);
		clazz.addProperty(pValue);
		pValue.setValue(vValue);

		table.addColumn(cValue);
	}
}
