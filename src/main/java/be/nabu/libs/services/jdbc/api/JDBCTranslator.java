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
}
