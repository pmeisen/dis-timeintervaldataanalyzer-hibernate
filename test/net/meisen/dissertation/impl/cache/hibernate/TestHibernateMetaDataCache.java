package net.meisen.dissertation.impl.cache.hibernate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.UUID;

import net.meisen.dissertation.help.Db;
import net.meisen.dissertation.help.LoaderBasedTest;
import net.meisen.dissertation.impl.datasets.SingleStaticDataSet;
import net.meisen.dissertation.model.data.DataStructure;
import net.meisen.dissertation.model.data.TidaModel;
import net.meisen.dissertation.model.data.metadata.IMetaDataCollection;
import net.meisen.dissertation.model.datastructure.IntervalStructureEntry;
import net.meisen.dissertation.model.datastructure.MetaStructureEntry;
import net.meisen.general.genmisc.types.Dates;
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
			db.openNewDb("testMetaDataDb", tmpDir, false);
		} else {
			db.addDb("testMetaDataDb", database);
		}
		db.setUpDb();
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
		TidaModel model;
		IMetaDataCollection mdc;

		setUp(null);
		model = m("/net/meisen/dissertation/impl/cache/hibernate/metaDataModel.xml");

		// check the empty cache
		assertEquals(0, model.getMetaDataCache().createMetaDataCollection()
				.size());

		mdc = model.getMetaDataCache().createMetaDataCollection();
		assertEquals(0, mdc.get("STRING").size());
		assertEquals(0, mdc.get("INT").size());
		assertEquals(0, mdc.get("LONG").size());

		// let's add an some values
		model.loadRecord(
				createStructure(),
				createDataset("01.01.2015", "02.01.2015", "value1", 50000,
						100000));
		model.loadRecord(
				createStructure(),
				createDataset("02.01.2015", "03.01.2015", "value2", 60000,
						200000));
		model.loadRecord(
				createStructure(),
				createDataset("03.01.2015", "04.01.2015", "value3", 70000,
						300000));
		mdc = model.getMetaDataCache().createMetaDataCollection();

		assertEquals(1, mdc.get("STRING").size());
		assertEquals(1, mdc.get("INT").size());
		assertEquals(1, mdc.get("LONG").size());
		assertEquals(3, mdc.sizeOfValues("STRING"));
		assertEquals(3, mdc.sizeOfValues("INT"));
		assertEquals(3, mdc.sizeOfValues("LONG"));

		createMetaData(model);
		mdc = model.getMetaDataCache().createMetaDataCollection();
		assertEquals(7, mdc.sizeOfValues("STRING"));
		assertEquals(1003, mdc.sizeOfValues("INT"));
		assertEquals(1003, mdc.sizeOfValues("LONG"));
	}

	/**
	 * Tests the caching of the {@code FileMetaDataCache}.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testReloading() throws IOException {
		TidaModel model;
		IMetaDataCollection mdc;

		setUp(null);

		model = m("/net/meisen/dissertation/impl/cache/hibernate/metaDataModel.xml");
		mdc = model.getMetaDataCache().createMetaDataCollection();
		assertEquals(0, mdc.size());

		createMetaData(model);
		mdc = model.getMetaDataCache().createMetaDataCollection();
		assertEquals(3, mdc.size());

		// unload the model
		this.loader.unloadAll();

		// now load the model again, the data (LX) should be available
		model = m("/net/meisen/dissertation/impl/cache/hibernate/metaDataModel.xml");

		mdc = model.getMetaDataCache().createMetaDataCollection();
		assertEquals(3, mdc.size());
		assertEquals(4, mdc.sizeOfValues("STRING"));
		assertEquals(1000, mdc.sizeOfValues("INT"));
		assertEquals(1000, mdc.sizeOfValues("LONG"));
	}

	/**
	 * Creates the {@code DataStructure} needed to add data to the
	 * {@code fileMetaDataCache}-model.
	 * 
	 * @return the created {@code DataStructure}
	 */
	protected DataStructure createStructure() {
		return new DataStructure(new IntervalStructureEntry("START", 1),
				new IntervalStructureEntry("END", 2), new MetaStructureEntry(
						"STRING", 3), new MetaStructureEntry("INT", 4),
				new MetaStructureEntry("LONG", 5));
	}

	/**
	 * Helper method to create a dataset for testing.
	 * 
	 * @param start
	 *            the start value
	 * @param end
	 *            the end value
	 * @param string
	 *            the STRING-descriptor value
	 * @param intVal
	 *            the INT-descriptor value
	 * @param longVal
	 *            the LONG-descriptor value
	 * 
	 * @return the created data-set
	 */
	protected SingleStaticDataSet createDataset(final String start,
			final String end, final String string, final int intVal,
			final long longVal) {
		try {
			return new SingleStaticDataSet(Dates.parseDate(end, "dd.MM.yyyy"),
					Dates.parseDate(end, "dd.MM.yyyy"), string, intVal, longVal);
		} catch (ParseException e) {
			fail(e.getMessage());
			return null;
		}
	}

	/**
	 * Clean up the created cache and the database.
	 */
	@After
	public void cleanUp() {
		super.unload();

		// shutdown the database
		if (db != null) {
			db.shutDownDb();
		}
		assertTrue(Files.deleteDir(tmpDir));
	}
}
