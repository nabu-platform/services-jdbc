/*
* Copyright (C) 2016 Alexander Verbruggen
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

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
