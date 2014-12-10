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

public abstract class HibernateBitmapIdBasedCache<T extends IBitmapIdCacheable>
		extends HibernateSessionManagerWithId<String> implements
		IBitmapIdCache<T>, IReferenceMechanismCache<BitmapId<?>, T> {
	
	protected String encodeBitmap(final BitmapId<?> bitmapId) {
		return DatatypeConverter.printBase64Binary(bitmapId.bytes());
	}

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
	public void remove() {
		super.remove();
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

	protected abstract void createAdditionalMappings(final Mappings mappings,
			final Table table, final RootClass clazz, final Dialect dialect);

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
