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

import javax.jws.WebParam;
import javax.jws.WebResult;
import javax.validation.constraints.NotNull;
import javax.xml.bind.annotation.XmlType;

public interface JDBCTranslator {
	
	@XmlType(propOrder = { "id", "name", "translation" })
	public static class Translation {
		private String translation;
		private String id, name;
		public String getId() {
			return id;
		}
		public void setId(String id) {
			this.id = id;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getTranslation() {
			return translation;
		}
		public void setTranslation(String translation) {
			this.translation = translation;
		}
	}
	
	public static class TranslationBinding {
		// the table the translations are in (mandatory)
		private String translationTable;
		// the field that contains the table name of the to-be-translated term (optional)
		private String fieldTable;
		// the field that contains the field name of the to-be-translated term (mandatory)
		private String fieldName;
		// the field that contains the id of the instance we are translating (mandatory)
		private String fieldId;
		// the field that actually contains the translation
		private String fieldTranslation;
		// the field that actually contains the language
		private String fieldLanguage;
		public String getTranslationTable() {
			return translationTable;
		}
		public void setTranslationTable(String translationTable) {
			this.translationTable = translationTable;
		}
		public String getFieldTable() {
			return fieldTable;
		}
		public void setFieldTable(String fieldTable) {
			this.fieldTable = fieldTable;
		}
		public String getFieldName() {
			return fieldName;
		}
		public void setFieldName(String fieldName) {
			this.fieldName = fieldName;
		}
		public String getFieldId() {
			return fieldId;
		}
		public void setFieldId(String fieldId) {
			this.fieldId = fieldId;
		}
		public String getFieldTranslation() {
			return fieldTranslation;
		}
		public void setFieldTranslation(String fieldTranslation) {
			this.fieldTranslation = fieldTranslation;
		}
		public String getFieldLanguage() {
			return fieldLanguage;
		}
		public void setFieldLanguage(String fieldLanguage) {
			this.fieldLanguage = fieldLanguage;
		}
	}
	
	@WebResult(name = "translations")
	public List<Translation> get(
		@WebParam(name = "connectionId") String connectionId, 
		@WebParam(name = "transactionId") String transactionId,
		@NotNull @WebParam(name = "language") String language, 
		@NotNull @WebParam(name = "ids") List<String> ids);
	
	public void set(
		@WebParam(name = "connectionId") String connectionId, 
		@WebParam(name = "transactionId") String transactionId, 
		@NotNull @WebParam(name = "language") String language, 
		@NotNull @WebParam(name = "translations") List<Translation> translations);
	
	// by default we don't have binding information
	@WebResult(name = "binding")
	public default TranslationBinding getBinding() {
		return null;
	}
	
	@WebResult(name = "mappedLanguage")
	// allow you to map the language we receive (e.g. "en") into a value that can be mapped (e.g. a masterdata entry id)
	public default String mapLanguage(@WebParam(name = "connectionId") String connectionId, @WebParam(name = "language") String language) {
		return language;
	}
}
