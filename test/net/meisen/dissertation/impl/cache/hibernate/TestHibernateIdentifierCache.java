package net.meisen.dissertation.impl.cache.hibernate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import net.meisen.dissertation.help.Db;
import net.meisen.dissertation.help.LoaderBasedTest;
import net.meisen.dissertation.model.data.TidaModel;
import net.meisen.dissertation.model.indexes.datarecord.slices.Bitmap;
import net.meisen.general.genmisc.exceptions.registry.DefaultExceptionRegistry;
import net.meisen.general.genmisc.types.Files;

import org.junit.After;
import org.junit.Test;

/**
 * Tests the implementation of a {@code HibernateIdentifierCache}.
 * 
 * @author pmeisen
 * 
 */
public class TestHibernateIdentifierCache extends LoaderBasedTest {

	private HibernateIdentifierCache cache;
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
		final HibernateIdentifierCacheConfig config = new HibernateIdentifierCacheConfig();
		config.setDriver("org.hsqldb.jdbcDriver");
		config.setUrl("jdbc:hsqldb:hsql://localhost:6666/testDb");
		config.setUsername("SA");
		config.setPassword("");

		// create the instance to be tested
		cache = new HibernateIdentifierCache();
		cache.setExceptionRegistry(excReg);
		cache.setConfig(config);
	}

	/**
	 * Tests the caching functionality of the implementation.
	 * 
	 * @throws IOException
	 *             if a file cannot be read
	 */
	@Test
	public void testCaching() throws IOException {
		setUp(null);

		final TidaModel model = m("/net/meisen/dissertation/impl/cache/hibernate/defaultModel.xml");
		cache.setIndexFactory(model.getIndexFactory());
		cache.initialize(model);

		assertEquals(-1, cache.getLastUsedIdentifier());
		assertEquals(model.getIndexFactory().createBitmap(),
				cache.getValidIdentifiers());

		// update the cache
		cache.markIdentifierAsUsed(5);
		cache.markIdentifierAsValid(2, 4, 5);

		assertEquals(5, cache.getLastUsedIdentifier());
		assertEquals(Bitmap.createBitmap(model.getIndexFactory(), 2, 4, 5),
				cache.getValidIdentifiers());
		cache.release();

		// re-init
		cache.initialize(model);
		assertEquals(5, cache.getLastUsedIdentifier());
		assertEquals(Bitmap.createBitmap(model.getIndexFactory(), 2, 4, 5),
				cache.getValidIdentifiers());
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
