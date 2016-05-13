package be.nabu.libs.services.jdbc.api;

import be.nabu.libs.artifacts.api.DataSourceProviderArtifact;

public interface DataSourceWithDialectProviderArtifact extends DataSourceProviderArtifact {
	public SQLDialect getDialect();
}
