package net.meisen.dissertation.impl.cache.hibernate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import net.meisen.dissertation.help.Db;
import net.meisen.dissertation.help.LoaderBasedTest;
import net.meisen.dissertation.model.data.TidaModel;
import net.meisen.dissertation.model.indexes.datarecord.IntervalIndex;
import net.meisen.dissertation.model.indexes.datarecord.MetaIndex;
import net.meisen.dissertation.model.indexes.datarecord.slices.Bitmap;
import net.meisen.dissertation.model.indexes.datarecord.slices.BitmapId;
import net.meisen.general.genmisc.exceptions.registry.DefaultExceptionRegistry;
import net.meisen.general.genmisc.types.Files;

import org.junit.After;
import org.junit.Test;

/**
 * Tests the implementation of a {@code HibernateBitmapCache}.
 * 
 * @author pmeisen
 * 
 */
public class TestHibernateBitmapCache extends LoaderBasedTest {

	private HibernateBitmapCache cache;
	private File tmpDir;
	private Db db;

	/**
	 * Setup a database and the cache.
	 * 
	 * @param database
	 *            the database to be loaded
	 * @throws IOException
	 *             if a file cannot be read
	 */
	public void setUp(final String database) throws IOException {

		// create the database
		tmpDir = new File(System.getProperty("java.io.tmpdir"), UUID
				.randomUUID().toString());
		db = new Db();
		if (database == null) {
			db.openNewDb("testDb", tmpDir, false);
		} else {
			db.addDb("testDb", database);
		}
		db.setUpDb();

		// create the HibernateDataRecordCache
		final DefaultExceptionRegistry excReg = new DefaultExceptionRegistry();

		// create a configuration
		final HibernateBitmapCacheConfig config = new HibernateBitmapCacheConfig();
		config.setDriver("org.hsqldb.jdbcDriver");
		config.setUrl("jdbc:hsqldb:hsql://localhost:6666/testDb");
		config.setUsername("SA");
		config.setPassword("");

		// create the instance to be tested
		cache = new HibernateBitmapCache();
		cache.setExceptionRegistry(excReg);
		cache.setConfig(config);
	}

	/**
	 * Tests the insertion of data into the cache.
	 * 
	 * @throws IOException
	 *             if a file cannot be read
	 */
	@Test
	public void testInsertion() throws IOException {
		setUp(null);

		final TidaModel model = m("/net/meisen/dissertation/impl/cache/hibernate/defaultModel.xml");
		cache.initialize(model);

		// create some IntervalIndex bitmaps
		for (int i = 0; i < 1000; i++) {
			final BitmapId<Integer> bitmapId = new BitmapId<Integer>(i,
					IntervalIndex.class);
			cache.cache(bitmapId,
					Bitmap.createBitmap(model.getIndexFactory(), i, i + 1));

			// validate the bitmap
			final Bitmap bitmap = cache.get(bitmapId);
			assertEquals(2, bitmap.determineCardinality());
			assertTrue(Arrays.binarySearch(bitmap.getIds(), i) > -1);
			assertTrue(Arrays.binarySearch(bitmap.getIds(), i + 1) > -1);
		}
		assertEquals(1000, cache.size());
		cache.release();
		cache.initialize(model);
		assertEquals(1000, cache.size());

		// create some MetaIndex bitmaps
		for (int i = 0; i < 1000; i++) {
			final BitmapId<Integer> bitmapId = new BitmapId<Integer>(i,
					MetaIndex.class, UUID.randomUUID().toString());
			cache.cache(bitmapId,
					Bitmap.createBitmap(model.getIndexFactory(), i, i + 1));

			// validate the bitmap
			final Bitmap bitmap = cache.get(bitmapId);
			assertEquals(2, bitmap.determineCardinality());
			assertTrue(Arrays.binarySearch(bitmap.getIds(), i) > -1);
			assertTrue(Arrays.binarySearch(bitmap.getIds(), i + 1) > -1);
		}
		assertEquals(2000, cache.size());
	}

	/**
	 * Tests the update of data within the cache.
	 * 
	 * @throws IOException
	 *             if a file cannot be read
	 */
	@Test
	public void testUpdate() throws IOException {
		setUp(null);

		final TidaModel model = m("/net/meisen/dissertation/impl/cache/hibernate/defaultModel.xml");
		cache.initialize(model);

		final BitmapId<Integer> bitmapId = new BitmapId<Integer>(5,
				IntervalIndex.class);
		for (int i = 0; i < 1000; i++) {
			Bitmap bitmap = cache.get(bitmapId);
			bitmap = bitmap == null ? Bitmap.createBitmap(
					model.getIndexFactory(), i) : bitmap.or(Bitmap
					.createBitmap(model.getIndexFactory(), i));

			// update the new instance
			cache.cache(bitmapId, bitmap);

			// get the cached instance and validate it
			cache.get(bitmapId);
			assertEquals(i + 1, bitmap.determineCardinality());
		}

		// finally re-init the whole cache
		cache.release();
		cache.initialize(model);
		assertEquals(1, cache.size());

		// validate the final bitmap stored
		final int[] res = cache.get(
				new BitmapId<Integer>(5, IntervalIndex.class)).getIds();
		for (int i = 0; i < 1000; i++) {
			assertTrue(Arrays.binarySearch(res, i) > -1);
		}
		assertFalse(Arrays.binarySearch(res, 1000) > -1);
	}

	/**
	 * Tests the iteration of data from the cache.
	 * 
	 * @throws IOException
	 *             if a file cannot be read
	 */
	@Test
	public void testIteration() throws IOException {
		setUp(null);

		final TidaModel model = m("/net/meisen/dissertation/impl/cache/hibernate/defaultModel.xml");
		cache.initialize(model);

		final List<BitmapId<?>> bitmapIds = new ArrayList<BitmapId<?>>();

		BitmapId<Integer> bitmapId;
		bitmapId = new BitmapId<Integer>(0, MetaIndex.class, "ÜÄÖß");
		cache.cache(bitmapId, Bitmap.createBitmap(model.getIndexFactory(), 0));
		bitmapIds.add(bitmapId);

		bitmapId = new BitmapId<Integer>(0, IntervalIndex.class);
		cache.cache(bitmapId, Bitmap.createBitmap(model.getIndexFactory(), 1));
		bitmapIds.add(bitmapId);

		// check the retrieved values
		for (BitmapId<?> id : cache) {
			assertTrue(bitmapIds.contains(id));
			bitmapIds.remove(id);
		}
		assertEquals(0, bitmapIds.size());
	}

	/**
	 * Clean up the created cache and the database.
	 */
	@After
	public void cleanUp() {

		// remove the cache
		if (cache != null) {
			cache.remove();
		}

		// shutdown the database
		if (db != null) {
			db.shutDownDb();
		}
		assertTrue(Files.deleteDir(tmpDir));
	}
}
