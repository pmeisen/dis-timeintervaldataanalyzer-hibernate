package net.meisen.dissertation.impl.cache.hibernate;

import java.util.Iterator;

import javax.xml.bind.DatatypeConverter;

import net.meisen.dissertation.jdbc.protocol.DataType;
import net.meisen.dissertation.model.cache.IBitmapIdCache;
import net.meisen.dissertation.model.cache.IBitmapIdCacheable;
import net.meisen.dissertation.model.cache.IReferenceMechanismCache;
import net.meisen.dissertation.model.indexes.datarecord.slices.BitmapId;

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
 * A base implementation of an {@code Hibernate} based bitmap using a
 * bitmap-identifier (see {@link BitmapId}) as identifier.
 * 
 * @author pmeisen
 * 
 * @param <T>
 *            the entity to be associated to the identifier
 */
public abstract class HibernateBitmapIdBasedCache<T extends IBitmapIdCacheable>
		extends HibernateSessionManager<String> implements IBitmapIdCache<T>,
		IReferenceMechanismCache<BitmapId<?>, T> {

	/**
	 * Encode the identifier of the bitmap to be used as string, without losing
	 * information.
	 * 
	 * @param bitmapId
	 *            the bitmap to be encoded
	 * 
	 * @return the encoded bitmap
	 * 
	 * @see DatatypeConverter#printBase64Binary(byte[])
	 */
	protected String encodeBitmap(final BitmapId<?> bitmapId) {
		return DatatypeConverter.printBase64Binary(bitmapId.bytes());
	}

	/**
	 * Decodes the encoded {@code bitmapId}.
	 * 
	 * @param bitmapId
	 *            the bitmap-identifier to be decoded
	 * 
	 * @return the original bitmap-identifier
	 */
	@SuppressWarnings("rawtypes")
	protected BitmapId<?> decodeBitmap(final String bitmapId) {
		return new BitmapId(DatatypeConverter.parseBase64Binary(bitmapId));
	}

	@Override
	public Iterator<BitmapId<?>> iterator() {
		final Iterator<String> it = createIterator();

		return new Iterator<BitmapId<?>>() {

			@Override
			public boolean hasNext() {
				return it.hasNext();
			}

			@Override
			public BitmapId<?> next() {
				return decodeBitmap(it.next());
			}

			@Override
			public void remove() {
				throw new IllegalStateException("Remove is not supported.");
			}
		};
	}

	@Override
	public boolean contains(final BitmapId<?> bitmapId) {
		return get(bitmapId) != null;
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
	protected abstract void createAdditionalMappings(final Mappings mappings,
			final Table table, final RootClass clazz, final Dialect dialect);

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
		column.setName(quote("bitmapId"));
		column.setNullable(false);
		column.setSqlTypeCode(DataType.STRING.getSqlType());
		column.setLength(BitmapId.getMaxBytesLength());

		final SimpleValue v = new SimpleValue(mappings);
		v.setTable(table);
		v.setTypeName(String.class.getName());
		v.addColumn(column);
		v.setIdentifierGeneratorStrategy(SimpleValue.DEFAULT_ID_GEN_STRATEGY);

		final Property p = new Property();
		p.setName("bitmapId");
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
}
