package net.meisen.dissertation.impl.cache.hibernate;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import net.meisen.general.genmisc.types.Objects;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;

import com.zaxxer.hikari.hibernate.HikariConnectionProvider;

/**
 * The basic configuration for an {@code Hibernate} configuration.
 * 
 * @author pmeisen
 * 
 */
public class HibernateConfig {

	/**
	 * Property used to define the maximal pool-size.
	 */
	public final static String PROP_MAXPOOLSIZE = "hibernate.hikari.maximumPoolSize";

	private String driver = null;
	private String url = null;
	private String username = null;
	private String password = null;
	private int commitSize = 100000;

	/**
	 * Helper method to create a map with the specified settings.
	 * 
	 * @return a map with configured settings
	 */
	protected Map<String, String> createSettings() {
		final Map<String, String> settings = new HashMap<String, String>();

		// if used without connectionPool
		// settings.put(AvailableSettings.DRIVER, getDriver());
		// settings.put(AvailableSettings.URL, getUrl());
		// settings.put(AvailableSettings.USER, getUsername());
		// settings.put(AvailableSettings.PASS, getPassword());
		settings.put(AvailableSettings.AUTOCOMMIT, "false");
		settings.put("hibernate.globally_quoted_identifiers", "true");
		settings.put("hibernate.hbm2ddl.auto", "update");

		// set default values
		settings.put(AvailableSettings.CONNECTION_PROVIDER,
				HikariConnectionProvider.class.getName());

		// set the connectionProvider settings
		settings.put(PROP_MAXPOOLSIZE, "10");
		settings.put("hibernate.hikari.driverClassName", getDriver());
		settings.put("hibernate.hikari.jdbcUrl", getUrl());
		settings.put("hibernate.hikari.username", getUsername());
		settings.put("hibernate.hikari.password", getPassword());

		return settings;
	}

	/**
	 * Creates a {@code Hibernate} valid configuration.
	 * 
	 * @return the {@code Hibernate} configuration
	 */
	public Configuration createHibernateConfig() {
		return createHibernateConfig(null);
	}

	/**
	 * Creates a {@code Hibernate} valid configuration.
	 * 
	 * @param override
	 *            properties set to override the specified configuration
	 * 
	 * @return the {@code Hibernate} configuration
	 */
	public Configuration createHibernateConfig(
			final Map<String, String> override) {
		final Map<String, String> settings = createSettings();
		if (override != null) {
			settings.putAll(override);
		}

		final Configuration config = new Configuration();

		for (final Entry<String, String> e : settings.entrySet()) {
			config.setProperty(e.getKey(), e.getValue());
		}

		return config;
	}

	/**
	 * Applies the configuration to the {@code builder}.
	 * 
	 * @param builder
	 *            the builder to aplly the configuration to
	 */
	public void applyToHibernateBuilder(
			final StandardServiceRegistryBuilder builder) {
		applyToHibernateBuilder(null, builder);
	}

	/**
	 * Applies the configuration to the {@code builder}.
	 * 
	 * @param override
	 *            properties set to override the specified configuration
	 * @param builder
	 *            the builder to aplly the configuration to
	 */
	public void applyToHibernateBuilder(final Map<String, String> override,
			final StandardServiceRegistryBuilder builder) {
		final Map<String, String> settings = createSettings();
		if (override != null) {
			settings.putAll(override);
		}

		for (final Entry<String, String> e : settings.entrySet()) {
			builder.applySetting(e.getKey(), e.getValue());
		}
	}

	/**
	 * Gets the specified driver.
	 * 
	 * @return the specified driver
	 */
	public String getDriver() {
		return driver;
	}

	/**
	 * Sets the driver.
	 * 
	 * @param driver
	 *            the driver
	 */
	public void setDriver(final String driver) {
		this.driver = driver;
	}

	/**
	 * Gets the specified url.
	 * 
	 * @return the specified url
	 */
	public String getUrl() {
		return url;
	}

	/**
	 * Sets the url.
	 * 
	 * @param url
	 *            the url
	 */
	public void setUrl(final String url) {
		this.url = url;
	}

	/**
	 * Gets the user-name specified.
	 * 
	 * @return the user-name specified
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * Sets the user-name to be used.
	 * 
	 * @param username
	 *            the user-name to be used
	 */
	public void setUsername(final String username) {
		this.username = username;
	}

	/**
	 * Gets the password specified.
	 * 
	 * @return the specified password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * Sets the password.
	 * 
	 * @param password
	 *            the password
	 */
	public void setPassword(final String password) {
		this.password = password;
	}

	/**
	 * Gets the specified commit-size.
	 * 
	 * @return the specified commit-size
	 */
	public int getCommitSize() {
		return commitSize;
	}

	/**
	 * Sets the commit-size.
	 * 
	 * @param commitSize
	 *            the commit-size
	 */
	public void setCommitSize(final int commitSize) {
		this.commitSize = commitSize;
	}

	@Override
	public int hashCode() {
		return Objects.generateHashCode(7, 43, getUrl(), getUsername());
	}

	@Override
	public boolean equals(final Object obj) {

		if (obj == this) {
			return true;
		} else if (obj == null) {
			return false;
		} else if (obj instanceof HibernateDataRecordCacheConfig) {
			final HibernateDataRecordCacheConfig c = (HibernateDataRecordCacheConfig) obj;
			return Objects.equals(getUrl(), c.getUrl())
					&& Objects.equals(getDriver(), c.getDriver())
					&& Objects.equals(getUsername(), c.getUsername())
					&& Objects.equals(getPassword(), c.getPassword());
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return getUrl() + " [" + getUsername() + ";" + getDriver() + "]";
	}
}
