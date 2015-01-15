package net.meisen.dissertation.impl.cache.hibernate;

import net.meisen.dissertation.model.data.TestMetaDataModel;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * All tests together as a {@link Suite}
 * 
 * @author pmeisen
 * 
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({ TestHibernateDataRecordCache.class,
		TestHibernateBitmapCache.class, TestHibernateMetaDataCache.class,
		TestHibernateFactDescriptorModelSetCache.class,
		TestMetaDataModel.class, TestHibernateIdentifierCache.class,
		TestInUsage.class })
public class AllTests {
	// nothing more to do here
}