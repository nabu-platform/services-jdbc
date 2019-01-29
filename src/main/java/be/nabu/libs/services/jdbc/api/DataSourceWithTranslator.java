package be.nabu.libs.services.jdbc.api;

import be.nabu.libs.artifacts.api.DataSourceProviderArtifact;

public interface DataSourceWithTranslator extends DataSourceProviderArtifact {
	public JDBCTranslator getTranslator();
	// the default language is not pushed to the external provider but stored in its proper place
	public String getDefaultLanguage();
}
