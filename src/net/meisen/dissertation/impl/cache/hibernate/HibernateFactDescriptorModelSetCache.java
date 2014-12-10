package net.meisen.dissertation.impl.cache.hibernate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import net.meisen.dissertation.model.cache.IBitmapIdCacheConfig;
import net.meisen.dissertation.model.data.TidaModel;
import net.meisen.dissertation.model.indexes.datarecord.slices.BitmapId;
import net.meisen.dissertation.model.indexes.datarecord.slices.FactDescriptorModelSet;
import net.meisen.general.genmisc.types.Streams;

import org.hibernate.cfg.Mappings;
import org.hibernate.dialect.Dialect;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.Property;
import org.hibernate.mapping.RootClass;
import org.hibernate.mapping.SimpleValue;
import org.hibernate.mapping.Table;

/**
 * Cache based on {@code Hibernate} used for {@code FactDescriptorModelSet}
 * instances.
 * 
 * @author pmeisen
 * 
 */
public class HibernateFactDescriptorModelSetCache extends
		HibernateBitmapIdBasedCache<FactDescriptorModelSet> {

	private HibernateFactDescriptorModelSetCacheConfig config;

	/**
	 * Default constructor.
	 */
	public HibernateFactDescriptorModelSetCache() {
		this.config = null;
	}

	@Override
	public FactDescriptorModelSet get(final BitmapId<?> id) {
		final String encBitmap = encodeBitmap(id);
		final Map<String, Object> map = getMap(encBitmap);
		if (map == null) {
			return new FactDescriptorModelSet();
		} else {
			final byte[] byteSet = (byte[]) map.get("factset");
			final DataInputStream in = new DataInputStream(
					new ByteArrayInputStream(byteSet));

			final FactDescriptorModelSet set = new FactDescriptorModelSet();
			try {
				set.deserialize(in);
			} catch (final IOException e) {
				exceptionRegistry.throwException(
						HibernateFactDescriptorModelSetCacheException.class,
						1000, id);
				return null;
			}

			return set;
		}
	}

	@Override
	public void setConfig(final IBitmapIdCacheConfig config) {
		if (config instanceof HibernateFactDescriptorModelSetCacheConfig) {
			this.config = (HibernateFactDescriptorModelSetCacheConfig) config;
		} else {

			// we have an invalid configuration
			this.config = null;
		}
	}

	@Override
	public void cache(final BitmapId<?> bitmapId,
			final FactDescriptorModelSet set) {
		final Map<String, Object> map = new HashMap<String, Object>();

		// get the encoded id
		final String id = encodeBitmap(bitmapId);
		map.put("bitmapId", id);

		// get the bitmap
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		final DataOutputStream w = new DataOutputStream(baos);
		try {
			set.serialize(w);
		} catch (final IOException e) {
			exceptionRegistry.throwException(
					HibernateFactDescriptorModelSetCacheException.class, 1001,
					id);
		}
		map.put("factset", baos.toByteArray());
		Streams.closeIO(baos);
		Streams.closeIO(w);

		// save the map
		saveMap(map, id);
	}

	@Override
	protected String createEntityName(final TidaModel model) {
		return "factsets_" + model.getId();
	}

	@Override
	protected Class<? extends RuntimeException>[] getExceptions() {
		@SuppressWarnings("unchecked")
		final Class<? extends RuntimeException>[] exp = new Class[] { HibernateFactDescriptorModelSetCacheException.class };

		return exp;
	}

	@Override
	protected HibernateFactDescriptorModelSetCacheConfig getConfig() {
		return config;
	}

	@Override
	protected void createAdditionalMappings(final Mappings mappings,
			final Table table, final RootClass clazz, final Dialect dialect) {

		// create the column
		final Column column = new Column();
		column.setName(quote("factset"));
		column.setNullable(false);
		column.setLength(Integer.MAX_VALUE);
		column.setSqlTypeCode(Types.VARBINARY);

		final SimpleValue v = new SimpleValue(mappings);
		v.setTable(table);
		v.setTypeName(byte[].class.getName());
		v.addColumn(column);

		final Property p = new Property();
		p.setName("factset");
		p.setLob(true);
		clazz.addProperty(p);
		p.setValue(v);

		table.addColumn(column);
	}
}