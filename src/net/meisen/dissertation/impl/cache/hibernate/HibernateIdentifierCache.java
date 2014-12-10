package net.meisen.dissertation.impl.cache.hibernate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import net.meisen.dissertation.impl.cache.BaseIdentifierCache;
import net.meisen.dissertation.impl.cache.BaseIdentifierCacheException;
import net.meisen.dissertation.jdbc.protocol.DataType;
import net.meisen.dissertation.model.cache.IIdentifierCacheConfig;
import net.meisen.dissertation.model.data.TidaModel;
import net.meisen.dissertation.model.indexes.BaseIndexFactory;
import net.meisen.dissertation.model.indexes.datarecord.slices.Bitmap;
import net.meisen.dissertation.model.indexes.datarecord.slices.BitmapId;
import net.meisen.general.genmisc.exceptions.registry.IExceptionRegistry;
import net.meisen.general.genmisc.types.Streams;

import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Mappings;
import org.hibernate.dialect.Dialect;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.PrimaryKey;
import org.hibernate.mapping.Property;
import org.hibernate.mapping.RootClass;
import org.hibernate.mapping.SimpleValue;
import org.hibernate.mapping.Table;
import org.hsqldb.types.Types;

public class HibernateIdentifierCache extends BaseIdentifierCache {

	private HibernateSessionManagerWithId<Integer> sessionManager;
	private HibernateIdentifierCacheConfig config;

	private Map<String, Object> currentData;

	public HibernateIdentifierCache() {
		this.sessionManager = null;
		this.config = null;
	}

	@Override
	public void initialize(final TidaModel model) {

		// create the manager
		sessionManager = new HibernateSessionManagerWithId<Integer>() {

			@Override
			protected String createEntityName(final TidaModel model) {
				return "identifier_" + model.getId();
			}

			@Override
			protected Class<? extends RuntimeException>[] getExceptions() {
				@SuppressWarnings("unchecked")
				final Class<? extends RuntimeException>[] exp = new Class[] { HibernateIdentifierCacheException.class };

				return exp;
			}

			@Override
			protected HibernateIdentifierCacheConfig getConfig() {
				return HibernateIdentifierCache.this.config;
			}

			@Override
			protected void defineMappings(final Configuration config,
					final Dialect dialect) {
				final String entityName = this.getEntityName();

				final Mappings mappings = config.createMappings();
				final Table table = mappings.addTable(null, null, entityName,
						null, false);

				final RootClass clazz = new RootClass();
				clazz.setEntityName(entityName);
				clazz.setJpaEntityName(entityName);
				clazz.setLazy(true);
				clazz.setTable(table);

				HibernateIdentifierCache.this.createKeyMapping(this, mappings,
						table, clazz, dialect);
				HibernateIdentifierCache.this.createAdditionalMappings(this,
						mappings, table, clazz, dialect);

				mappings.addClass(clazz);
			}
		};

		// initialize the model
		sessionManager.setExceptionRegistry(exceptionRegistry);
		sessionManager.initialize(model);

		// get the currentData
		currentData = sessionManager.getMap(0);
		if (currentData == null) {
			currentData = new HashMap<String, Object>();
			currentData.put("key", 0);
			currentData.put("lastused", -1);
			currentData.put("bitmap",
					getByteBitmap(indexFactory.createBitmap()));
		}

		// initialize with the currentData
		final int lastUsedIdentifier = (Integer) currentData.get("lastused");
		final byte[] bitmap = (byte[]) currentData.get("bitmap");
		initialize(lastUsedIdentifier, getBitmap(bitmap));

		markAsInitialized();
	}

	public void setExceptionRegistry(final IExceptionRegistry exceptionRegistry) {
		this.exceptionRegistry = exceptionRegistry;
	}

	public void setIndexFactory(final BaseIndexFactory indexFactory) {
		this.indexFactory = indexFactory;
	}

	protected byte[] getByteBitmap(final Bitmap bitmap) {

		// get the bitmap
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		final DataOutputStream w = new DataOutputStream(baos);
		try {
			bitmap.serialize(w);
		} catch (final IOException e) {
			exceptionRegistry.throwException(
					HibernateIdentifierCacheException.class, 1001, e);
			return null;
		} finally {
			Streams.closeIO(baos);
			Streams.closeIO(w);
		}

		return baos.toByteArray();
	}

	protected Bitmap getBitmap(final byte[] byteBitmap) {
		final DataInputStream in = new DataInputStream(
				new ByteArrayInputStream(byteBitmap));

		try {
			return Bitmap.createFromInput(indexFactory, in);
		} catch (final IOException e) {
			exceptionRegistry.throwException(
					HibernateIdentifierCacheException.class, 1000, e);
			return null;
		}
	}

	@Override
	public void setConfig(final IIdentifierCacheConfig config)
			throws BaseIdentifierCacheException {
		if (config instanceof HibernateIdentifierCacheConfig) {
			this.config = (HibernateIdentifierCacheConfig) config;
		} else {

			// we have an invalid configuration
			this.config = null;
		}
	}

	@Override
	public void remove() {
		sessionManager.remove();
	}

	@Override
	public void release() {
		super.release();
		sessionManager.release();
	}

	@Override
	protected void cacheBitmap(final Bitmap newBitmap) {
		currentData.put("bitmap", getByteBitmap(newBitmap));
		sessionManager.saveMap(currentData, 0);
	}

	@Override
	protected void cacheIdentifier(final int lastUsedIdentifier) {
		currentData.put("lastused", lastUsedIdentifier);
		sessionManager.saveMap(currentData, 0);
	}

	protected void createAdditionalMappings(
			final HibernateSessionManagerWithId<Integer> manager,
			final Mappings mappings, final Table table, final RootClass clazz,
			final Dialect dialect) {

		// create the column for the bitmap
		final Column cBitmap = new Column();
		cBitmap.setName(manager.quote("bitmap"));
		cBitmap.setNullable(false);
		cBitmap.setLength(Integer.MAX_VALUE);
		cBitmap.setSqlTypeCode(Types.VARBINARY);

		final SimpleValue vBitmap = new SimpleValue(mappings);
		vBitmap.setTable(table);
		vBitmap.setTypeName(byte[].class.getName());
		vBitmap.addColumn(cBitmap);

		final Property pBitmap = new Property();
		pBitmap.setName("bitmap");
		pBitmap.setLob(true);
		clazz.addProperty(pBitmap);
		pBitmap.setValue(vBitmap);

		table.addColumn(cBitmap);

		// create the column for the lastUsed
		final Column cLastUsed = new Column();
		cLastUsed.setName(manager.quote("lastused"));
		cLastUsed.setNullable(false);
		cLastUsed.setSqlTypeCode(DataType.INT.getSqlType());

		final SimpleValue vLastUsed = new SimpleValue(mappings);
		vLastUsed.setTable(table);
		vLastUsed.setTypeName(Integer.class.getName());
		vLastUsed.addColumn(cLastUsed);

		final Property pLastUsed = new Property();
		pLastUsed.setName("lastused");
		clazz.addProperty(pLastUsed);
		pLastUsed.setValue(vLastUsed);

		table.addColumn(cLastUsed);
	}

	protected void createKeyMapping(
			final HibernateSessionManagerWithId<Integer> manager,
			final Mappings mappings, final Table table, final RootClass clazz,
			final Dialect dialect) {
		final Column column = new Column();
		column.setName(manager.quote("key"));
		column.setNullable(false);
		column.setSqlTypeCode(DataType.INT.getSqlType());
		column.setLength(BitmapId.getMaxBytesLength());

		final SimpleValue v = new SimpleValue(mappings);
		v.setTable(table);
		v.setTypeName(Integer.class.getName());
		v.addColumn(column);
		v.setIdentifierGeneratorStrategy(SimpleValue.DEFAULT_ID_GEN_STRATEGY);

		final Property p = new Property();
		p.setName("key");
		p.setValue(v);
		p.setInsertable(false);
		p.setUpdateable(false);

		final PrimaryKey primaryKey = new PrimaryKey();
		primaryKey.setName("PK_" + manager.getEntityName());
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
