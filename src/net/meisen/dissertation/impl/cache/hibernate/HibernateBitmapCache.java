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
import net.meisen.dissertation.model.indexes.BaseIndexFactory;
import net.meisen.dissertation.model.indexes.datarecord.slices.Bitmap;
import net.meisen.dissertation.model.indexes.datarecord.slices.BitmapId;
import net.meisen.general.genmisc.types.Streams;

import org.hibernate.cfg.Mappings;
import org.hibernate.dialect.Dialect;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.Property;
import org.hibernate.mapping.RootClass;
import org.hibernate.mapping.SimpleValue;
import org.hibernate.mapping.Table;

/**
 * Implementation of a {@code Cache} caching bitmap instances associated to
 * identifiers.
 * 
 * @author pmeisen
 * 
 */
public class HibernateBitmapCache extends HibernateBitmapIdBasedCache<Bitmap> {

	private HibernateBitmapCacheConfig config;
	private BaseIndexFactory idxFactory;

	/**
	 * Default constructor.
	 */
	public HibernateBitmapCache() {
		this.config = null;
		this.idxFactory = null;
	}

	@Override
	public void initialize(final TidaModel model) {

		// get the needed values
		this.idxFactory = model.getIndexFactory();

		// now initialize
		super.initialize(model);
	}

	@Override
	public Bitmap get(final BitmapId<?> id) {
		final String encBitmap = encodeBitmap(id);
		final Map<String, Object> map = getMap(encBitmap);
		if (map == null) {
			return idxFactory.createBitmap();
		} else {

			final byte[] byteBitmap = (byte[]) map.get("bitmap");
			final DataInputStream in = new DataInputStream(
					new ByteArrayInputStream(byteBitmap));

			try {
				return Bitmap.createFromInput(idxFactory, in);
			} catch (final IOException e) {
				exceptionRegistry.throwException(
						HibernateBitmapCacheException.class, 1000, id);
				return null;
			}
		}
	}

	@Override
	public void setConfig(final IBitmapIdCacheConfig config) {
		if (config instanceof HibernateBitmapCacheConfig) {
			this.config = (HibernateBitmapCacheConfig) config;
		} else {

			// we have an invalid configuration
			this.config = null;
		}
	}

	@Override
	public void cache(final BitmapId<?> bitmapId, final Bitmap bitmap) {
		final Map<String, Object> map = new HashMap<String, Object>();

		// get the encoded id
		final String id = encodeBitmap(bitmapId);
		map.put("bitmapId", id);

		// get the bitmap
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		final DataOutputStream w = new DataOutputStream(baos);
		try {
			bitmap.serialize(w);
		} catch (final IOException e) {
			exceptionRegistry.throwException(
					HibernateBitmapCacheException.class, 1001, id);
		}
		map.put("bitmap", baos.toByteArray());
		Streams.closeIO(baos);
		Streams.closeIO(w);

		// save the map
		saveMap(map, id);
	}

	@Override
	protected String createEntityName(final TidaModel model) {
		return "bitmaps_" + model.getId();
	}

	@Override
	protected Class<? extends RuntimeException>[] getExceptions() {
		@SuppressWarnings("unchecked")
		final Class<? extends RuntimeException>[] exp = new Class[] { HibernateBitmapCacheException.class };

		return exp;
	}

	@Override
	protected HibernateConfig getConfig() {
		return config;
	}

	@Override
	protected void createAdditionalMappings(final Mappings mappings,
			final Table table, final RootClass clazz, final Dialect dialect) {

		// create the column
		final Column column = new Column();
		column.setName(quote("bitmap"));
		column.setNullable(false);
		column.setLength(Integer.MAX_VALUE);
		column.setSqlTypeCode(Types.VARBINARY);

		final SimpleValue v = new SimpleValue(mappings);
		v.setTable(table);
		v.setTypeName(byte[].class.getName());
		v.addColumn(column);

		final Property p = new Property();
		p.setName("bitmap");
		p.setLob(true);
		clazz.addProperty(p);
		p.setValue(v);

		table.addColumn(column);
	}
}
