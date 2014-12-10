package net.meisen.dissertation.impl.cache.hibernate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import gnu.trove.list.array.TIntArrayList;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import net.meisen.dissertation.help.Db;
import net.meisen.dissertation.help.LoaderBasedTest;
import net.meisen.dissertation.model.data.FieldNameGenerator;
import net.meisen.dissertation.model.data.TidaModel;
import net.meisen.dissertation.model.util.IIntIterator;
import net.meisen.general.genmisc.exceptions.registry.DefaultExceptionRegistry;
import net.meisen.general.genmisc.types.Dates;
import net.meisen.general.genmisc.types.Files;

import org.junit.After;
import org.junit.Test;

/**
 * Tests the implementation of a {@code HibernateDataRecordCache}.
 * 
 * @author pmeisen
 * 
 */
public class TestHibernateDataRecordCache extends LoaderBasedTest {

	private HibernateDataRecordCache cache;
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
		final HibernateDataRecordCacheConfig config = new HibernateDataRecordCacheConfig();
		config.setDriver("org.hsqldb.jdbcDriver");
		config.setUrl("jdbc:hsqldb:hsql://localhost:6666/testDb");
		config.setUsername("SA");
		config.setPassword("");

		// create the instance to be tested
		cache = new HibernateDataRecordCache();
		cache.setExceptionRegistry(excReg);
		cache.setConfig(config);
	}

	/**
	 * Tests the insertion of several values
	 * 
	 * @throws ParseException
	 *             if a date cannot be parsed
	 * @throws IOException
	 *             if set-up fails
	 */
	@Test
	public void testInsertion() throws ParseException, IOException {
		setUp(null);

		final TidaModel model = m("/net/meisen/dissertation/impl/cache/hibernate/defaultModel.xml");
		cache.initialize(model);

		final FieldNameGenerator fg = FieldNameGenerator.get();

		for (int i = 0; i < 1000; i++) {
			final Map<String, Object> map = new HashMap<String, Object>();
			map.put(fg.getIdFieldName(), i);
			map.put(fg.getIntervalStartFieldName(), Dates.parseDate(
					"01.02.2015 07:56:00", "dd.MM.yyyy HH:mm:ss"));
			map.put(fg.getIntervalEndFieldName(), Dates.parseDate(
					"01.02.2015 08:46:00", "dd.MM.yyyy HH:mm:ss"));
			map.put("STRING", "TestValue");
			map.put("INT", 2);
			map.put("LONG", 5l);
			cache.cache(map);

			assertEquals(i + 1, cache.size());
		}

		// check the size of added values
		assertEquals(1000, cache.size());

		// check the values
		for (int i = 0; i < 1000; i++) {
			final Object[] rec = cache.get(i);
			assertEquals(rec[0], i);
			assertEquals(rec[1], Dates.parseDate("01.02.2015 07:56:00",
					"dd.MM.yyyy HH:mm:ss"));
			assertEquals(rec[2], Dates.parseDate("01.02.2015 08:46:00",
					"dd.MM.yyyy HH:mm:ss"));
			assertEquals(rec[3], 2);
			assertEquals(rec[4], 5l);
			assertEquals(rec[5], "TestValue");
		}
	}

	/**
	 * Tests the reloading of several values
	 * 
	 * @throws IOException
	 *             if set-up fails
	 * @throws ParseException
	 *             if a date could not be parsed
	 */
	@Test
	public void testReloading() throws IOException, ParseException {
		setUp("/net/meisen/dissertation/impl/cache/hibernate/hsqldb/testDbRecords.zip");

		final TidaModel model = m("/net/meisen/dissertation/impl/cache/hibernate/defaultModel.xml");
		cache.initialize(model);

		// check the size of added values
		assertEquals(1000, cache.size());

		// check the values
		for (int i = 0; i < 1000; i++) {
			final Object[] rec = cache.get(i);
			assertEquals(rec[0], i);
			assertEquals(rec[1], Dates.parseDate("01.02.2015 07:56:00",
					"dd.MM.yyyy HH:mm:ss"));
			assertEquals(rec[2], Dates.parseDate("01.02.2015 08:46:00",
					"dd.MM.yyyy HH:mm:ss"));
			assertEquals(rec[3], 2);
			assertEquals(rec[4], 5l);
			assertEquals(rec[5], "TestValue");
		}
	}

	/**
	 * Tests the iteration over identifiers.
	 * 
	 * @throws IOException
	 *             if set-up fails
	 */
	@Test
	public void testIteration() throws IOException {
		setUp("/net/meisen/dissertation/impl/cache/hibernate/hsqldb/testDbRecords.zip");

		final TidaModel model = m("/net/meisen/dissertation/impl/cache/hibernate/defaultModel.xml");
		cache.initialize(model);

		// check the size of added values
		assertEquals(1000, cache.size());

		// create a list with expected values
		final TIntArrayList list = new TIntArrayList();
		for (int i = 0; i < 1000; i++) {
			list.add(i);
		}

		// modify the list for each value iterated
		final IIntIterator it = cache.intIterator();
		while (it.hasNext()) {
			final int i = it.next();
			assertTrue(list.contains(i));
			list.remove(i);
		}

		// make sure the list was completely covered
		assertEquals(list.toString(), 0, list.size());
	}

	/**
	 * Clean up the created cache and the database.
	 */
	@After
	public void cleanUp() {

		// remove the cache
		cache.remove();

		// shutdown the database
		if (db != null) {
			db.shutDownDb();
		}
		assertTrue(Files.deleteDir(tmpDir));
	}
}
