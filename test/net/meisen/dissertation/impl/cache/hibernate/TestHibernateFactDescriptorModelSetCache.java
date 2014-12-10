package net.meisen.dissertation.impl.cache.hibernate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import net.meisen.dissertation.help.Db;
import net.meisen.dissertation.help.LoaderBasedTest;
import net.meisen.dissertation.model.data.TidaModel;
import net.meisen.dissertation.model.indexes.datarecord.IntervalIndex;
import net.meisen.dissertation.model.indexes.datarecord.MetaIndex;
import net.meisen.dissertation.model.indexes.datarecord.slices.BitmapId;
import net.meisen.dissertation.model.indexes.datarecord.slices.FactDescriptorModelSet;
import net.meisen.general.genmisc.exceptions.registry.DefaultExceptionRegistry;
import net.meisen.general.genmisc.types.Files;

import org.junit.After;
import org.junit.Test;

/**
 * Tests the implementation of a {@code HibernateDataRecordCache}.
 * 
 * @author pmeisen
 * 
 */
public class TestHibernateFactDescriptorModelSetCache extends LoaderBasedTest {

	private HibernateFactDescriptorModelSetCache cache;
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
		final HibernateFactDescriptorModelSetCacheConfig config = new HibernateFactDescriptorModelSetCacheConfig();
		config.setDriver("org.hsqldb.jdbcDriver");
		config.setUrl("jdbc:hsqldb:hsql://localhost:6666/testDb");
		config.setUsername("SA");
		config.setPassword("");

		// create the instance to be tested
		cache = new HibernateFactDescriptorModelSetCache();
		cache.setExceptionRegistry(excReg);
		cache.setConfig(config);
	}

	/**
	 * Tests the insertion of data into the cache.
	 * 
	 * @throws ParseException
	 *             if a date cannot be parsed
	 * @throws IOException
	 *             if a file cannot be read
	 */
	@Test
	public void testInsertion() throws ParseException, IOException {
		setUp(null);

		final TidaModel model = m("/net/meisen/dissertation/impl/cache/hibernate/defaultModel.xml");
		cache.initialize(model);

		// add some descriptors
		model.getMetaDataModel().createDescriptor("STRING", "Dog");
		model.getMetaDataModel().createDescriptor("STRING", "Cat");
		model.getMetaDataModel().createDescriptor("STRING", "Mouse");
		for (int i = 0; i < 1000; i++) {
			model.getMetaDataModel().createDescriptor("INT", i);
		}
		for (long i = Long.MAX_VALUE; i > Long.MAX_VALUE - 1000; i--) {
			model.getMetaDataModel().createDescriptor("LONG", i);
		}

		// create some IntervalIndex bitmaps
		for (int i = 0; i < 1000; i++) {
			final BitmapId<Integer> bitmapId = new BitmapId<Integer>(i,
					IntervalIndex.class);
			final FactDescriptorModelSet insertSet = new FactDescriptorModelSet();
			insertSet.setDescriptors(model.getMetaDataModel().getDescriptors());
			cache.cache(bitmapId, insertSet);

			// validate the bitmap
			final FactDescriptorModelSet set = cache.get(bitmapId);
			assertEquals(insertSet, set);
		}
		assertEquals(1000, cache.size());
		cache.release();
		cache.initialize(model);
		assertEquals(1000, cache.size());

		// create some MetaIndex bitmaps
		for (int i = 0; i < 1000; i++) {
			final BitmapId<Integer> bitmapId = new BitmapId<Integer>(i,
					MetaIndex.class, UUID.randomUUID().toString());
			final FactDescriptorModelSet insertSet = new FactDescriptorModelSet();
			insertSet.setDescriptors(model.getMetaDataModel().getDescriptors());
			cache.cache(bitmapId, insertSet);

			// validate the bitmap
			final FactDescriptorModelSet set = cache.get(bitmapId);
			assertEquals(insertSet, set);
		}
		assertEquals(2000, cache.size());
	}

	/**
	 * Tests the update of data within the cache.
	 * 
	 * @throws ParseException
	 *             if a date cannot be parsed
	 * @throws IOException
	 *             if a file cannot be read
	 */
	@Test
	public void testUpdate() throws ParseException, IOException {
		setUp(null);

		final TidaModel model = m("/net/meisen/dissertation/impl/cache/hibernate/defaultModel.xml");
		cache.initialize(model);

		// add some descriptors
		model.getMetaDataModel().createDescriptor("STRING", "Dog");

		// create some IntervalIndex bitmaps
		for (int i = 0; i < 1000; i++) {
			final BitmapId<Integer> bitmapId = new BitmapId<Integer>(i,
					IntervalIndex.class);
			final FactDescriptorModelSet insertSet = new FactDescriptorModelSet();
			insertSet.setDescriptors(model.getMetaDataModel().getDescriptors());
			cache.cache(bitmapId, insertSet);

			// validate the bitmap
			final FactDescriptorModelSet set = cache.get(bitmapId);
			assertEquals(insertSet, set);
		}
		assertEquals(1000, cache.size());
		cache.release();
		cache.initialize(model);
		assertEquals(1000, cache.size());

		// add some more descriptors
		model.getMetaDataModel().createDescriptor("STRING", "Cat");
		model.getMetaDataModel().createDescriptor("STRING", "Mouse");

		// create some MetaIndex bitmaps
		for (int i = 0; i < 1000; i++) {
			final BitmapId<Integer> bitmapId = new BitmapId<Integer>(i,
					IntervalIndex.class);
			final FactDescriptorModelSet updatedSet = new FactDescriptorModelSet();
			updatedSet
					.setDescriptors(model.getMetaDataModel().getDescriptors());
			cache.cache(bitmapId, updatedSet);

			// validate the bitmap
			final FactDescriptorModelSet set = cache.get(bitmapId);
			assertEquals(updatedSet, set);
		}
		assertEquals(1000, cache.size());
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
		cache.cache(bitmapId, new FactDescriptorModelSet());
		bitmapIds.add(bitmapId);

		bitmapId = new BitmapId<Integer>(0, IntervalIndex.class);
		cache.cache(bitmapId, new FactDescriptorModelSet());
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
