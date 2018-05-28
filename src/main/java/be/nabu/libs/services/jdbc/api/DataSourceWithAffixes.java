package be.nabu.libs.services.jdbc.api;

import java.util.List;

import javax.xml.bind.annotation.XmlType;

import be.nabu.libs.artifacts.api.DataSourceProviderArtifact;

public interface DataSourceWithAffixes extends DataSourceProviderArtifact {

	public List<AffixMapping> getAffixes();
	
	@XmlType(propOrder={"context", "affix", "tables"})
	public static class AffixMapping {
		private String context, affix;
		private List<String> tables;

		public String getContext() {
			return context;
		}

		public void setContext(String context) {
			this.context = context;
		}

		public String getAffix() {
			return affix;
		}

		public void setAffix(String affix) {
			this.affix = affix;
		}

		public List<String> getTables() {
			return tables;
		}

		public void setTables(List<String> tables) {
			this.tables = tables;
		}
		
	}
}
