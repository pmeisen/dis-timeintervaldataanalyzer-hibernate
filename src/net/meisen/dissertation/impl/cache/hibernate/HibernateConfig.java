package net.meisen.dissertation.impl.cache.hibernate;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import net.meisen.general.genmisc.types.Objects;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;

import com.zaxxer.hikari.hibernate.HikariConnectionProvider;

public class HibernateConfig {
	public final static String PROP_MAXPOOLSIZE = "hibernate.hikari.maximumPoolSize";

	private String driver = null;
	private String url = null;
	private String username = null;
	private String password = null;
	private int commitSize = 100000;

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

	public Configuration createHibernateConfig() {
		return createHibernateConfig(null);
	}

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

	public void applyToHibernateBuilder(
			final StandardServiceRegistryBuilder builder) {
		applyToHibernateBuilder(null, builder);
	}

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

	public String getDriver() {
		return driver;
	}

	public void setDriver(final String driver) {
		this.driver = driver;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(final String url) {
		this.url = url;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(final String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(final String password) {
		this.password = password;
	}

	public int getCommitSize() {
		return commitSize;
	}

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
