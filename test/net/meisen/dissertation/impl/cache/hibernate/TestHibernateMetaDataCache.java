package net.meisen.dissertation.impl.cache.hibernate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import net.meisen.dissertation.help.Db;
import net.meisen.dissertation.help.LoaderBasedTest;
import net.meisen.dissertation.impl.cache.UtilMetaDataCache;
import net.meisen.dissertation.model.data.TidaModel;
import net.meisen.dissertation.model.data.metadata.MetaDataCollection;
import net.meisen.general.genmisc.exceptions.registry.DefaultExceptionRegistry;
import net.meisen.general.genmisc.types.Files;

import org.junit.After;
import org.junit.Test;

/**
 * Tests the implementation of a {@code HibernateMetaDataCache}.
 * 
 * @author pmeisen
 * 
 */
public class TestHibernateMetaDataCache extends LoaderBasedTest {

	private HibernateMetaDataCache cache;
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
		final HibernateMetaDataCacheConfig config = new HibernateMetaDataCacheConfig();
		config.setDriver("org.hsqldb.jdbcDriver");
		config.setUrl("jdbc:hsqldb:hsql://localhost:6666/testDb");
		config.setUsername("SA");
		config.setPassword("");

		// create the instance to be tested
		cache = new HibernateMetaDataCache();
		cache.setExceptionRegistry(excReg);
		cache.setConfig(config);
	}

	/**
	 * Helper method to create some meta-data within the model.
	 * 
	 * @param model
	 *            the model to create meta-data in
	 */
	protected void createMetaData(final TidaModel model) {

		// add some descriptors
		model.getMetaDataModel().createDescriptor("STRING", "Dog");
		model.getMetaDataModel().createDescriptor("STRING", "Cat");
		model.getMetaDataModel().createDescriptor("STRING", "Mouse");
		model.getMetaDataModel().createDescriptor("STRING", "ÜÄÖß");
		for (int i = 0; i < 1000; i++) {
			model.getMetaDataModel().createDescriptor("INT", i);
		}
		for (long i = Long.MAX_VALUE; i > Long.MAX_VALUE - 1000; i--) {
			model.getMetaDataModel().createDescriptor("LONG", i);
		}
	}

	/**
	 * Tests the caching implementation.
	 * 
	 * @throws IOException
	 *             if the file of the model cannot be read
	 */
	@Test
	public void testCaching() throws IOException {
		setUp(null);

		final TidaModel model = m("/net/meisen/dissertation/impl/cache/hibernate/defaultModel.xml");
		createMetaData(model);

		// check the empty cache
		cache.initialize(model);
		assertEquals(0, cache.size());

		// add some values to the cache
		cache.cacheMetaDataModel(model.getMetaDataModel());
		assertEquals(model.getMetaDataModel().getDescriptors().size(),
				cache.size());

		// get the values from the cache
		final MetaDataCollection coll = cache.createMetaDataCollection();
		assertEquals(UtilMetaDataCache.createCollectionForModel(model
				.getMetaDataModel()), coll);
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
