package net.meisen.dissertation.impl.cache.hibernate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.UUID;

import net.meisen.dissertation.help.Db;
import net.meisen.dissertation.model.data.FieldNameGenerator;
import net.meisen.dissertation.server.BaseTestWithServerConnection;
import net.meisen.general.genmisc.types.Dates;
import net.meisen.general.genmisc.types.Files;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests the usage of the full stack of hibernate caches.
 * 
 * @author pmeisen
 * 
 */
public class TestInUsage extends BaseTestWithServerConnection {
	private static final FieldNameGenerator fg = FieldNameGenerator.get();
	private static final File tmpDir = new File(
			System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());
	private static Db db;

	/**
	 * Starts the database to be used
	 * 
	 * @throws IOException
	 *             if the database cannot be started
	 */
	@BeforeClass
	public static void initDb() throws IOException {
		db = new Db();
		db.openNewDb("testDb", tmpDir, false);
		db.setUpDb();
	}

	/**
	 * Shuts the database down and cleans up.
	 */
	@AfterClass
	public static void shutdownDb() {
		db.shutDownDb();
		assertTrue(Files.deleteDir(tmpDir));
	}

	@Override
	public int getTsqlPort() {
		return 6668;
	}

	@Override
	public boolean isTSQL() {
		return true;
	}

	/**
	 * Tests the insertion and the caching of data
	 * 
	 * @throws SQLException
	 *             if a statement cannot be executed
	 * @throws ParseException
	 *             if parsing of a date fails
	 */
	@Test
	public void testDataInsertionAndCaching() throws SQLException,
			ParseException {
		Statement stmt;
		ResultSet res;
		stmt = conn.createStatement();

		// load the model and add some data
		stmt.executeUpdate("LOAD FROM 'classpath://net/meisen/dissertation/impl/cache/hibernate/typeModel.xml'");
		for (int i = 0; i < 100; i++) {
			stmt.executeUpdate("INSERT INTO typeModel ([START], [END], LONG, INT, STRING) VALUES (01.01.2015 08:00:00, 01.01.2015 08:07:00, '"
					+ Long.MIN_VALUE
					+ "', '"
					+ Integer.MAX_VALUE
					+ "', 'DIES IST EIN Ä TEST')");
		}

		// make sure everything is added
		res = stmt.executeQuery("SELECT COUNT(RECORDS) FROM typeModel");
		assertTrue(res.next());
		assertEquals(100, res.getInt(1));
		assertFalse(res.next());

		// next unload the model and reload it
		stmt.executeUpdate("UNLOAD typeModel");
		stmt.executeUpdate("LOAD FROM 'classpath://net/meisen/dissertation/impl/cache/hibernate/typeModel.xml'");

		// make sure everything is loaded
		res = stmt.executeQuery("SELECT COUNT(RECORDS) FROM typeModel");
		assertTrue(res.next());
		assertEquals(100, res.getInt(1));
		assertFalse(res.next());

		// check the values
		res = stmt.executeQuery("SELECT RECORDS FROM typeModel");
		while (res.next()) {
			assertEquals(Dates.parseDate("01.01.2015 08:00:00",
					"dd.MM.yyyy HH:mm:ss"), res.getDate(fg
					.getIntervalStartFieldName()));
			assertEquals(Dates.parseDate("01.01.2015 08:07:00",
					"dd.MM.yyyy HH:mm:ss"), res.getDate(fg
					.getIntervalEndFieldName()));

			assertEquals(Long.MIN_VALUE, res.getLong("LONG"));
			assertEquals(Integer.MAX_VALUE, res.getInt("INT"));
			assertEquals("DIES IST EIN Ä TEST", res.getString("STRING"));
		}

		// delete the model and reload it nothing should be there
		stmt.executeUpdate("DROP MODEL typeModel");
		stmt.executeUpdate("LOAD FROM 'classpath://net/meisen/dissertation/impl/cache/hibernate/typeModel.xml'");
		res = stmt.executeQuery("SELECT COUNT(RECORDS) FROM typeModel");

		// check the records
		assertTrue(res.next());
		assertEquals(0, res.getInt(1));
		assertFalse(res.next());

		stmt.close();
	}

	/**
	 * Removes the created model from the database.
	 * 
	 * @throws SQLException
	 *             if the statement fails
	 */
	@After
	public void cleanUp() throws SQLException {
		final Statement stmt = conn.createStatement();
		stmt.executeUpdate("DROP MODEL typeModel");
		stmt.close();
	}
}
