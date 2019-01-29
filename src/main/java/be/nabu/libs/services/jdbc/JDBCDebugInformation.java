package be.nabu.libs.services.jdbc;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "jdbc")
public class JDBCDebugInformation {
	
	private String connectionId, transactionId, sql, totalCountSql;
	private Integer inputAmount, outputAmount;
	private Long executionDuration, mappingDuration;
	
	public String getTransactionId() {
		return transactionId;
	}
	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}
	public String getSql() {
		return sql;
	}
	public void setSql(String sql) {
		this.sql = sql;
	}
	public Integer getInputAmount() {
		return inputAmount;
	}
	public void setInputAmount(Integer inputAmount) {
		this.inputAmount = inputAmount;
	}
	public Integer getOutputAmount() {
		return outputAmount;
	}
	public void setOutputAmount(Integer outputAmount) {
		this.outputAmount = outputAmount;
	}
	public String getConnectionId() {
		return connectionId;
	}
	public void setConnectionId(String connectionId) {
		this.connectionId = connectionId;
	}
	public Long getExecutionDuration() {
		return executionDuration;
	}
	public void setExecutionDuration(Long executionDuration) {
		this.executionDuration = executionDuration;
	}
	public Long getMappingDuration() {
		return mappingDuration;
	}
	public void setMappingDuration(Long mappingDuration) {
		this.mappingDuration = mappingDuration;
	}
	public String getTotalCountSql() {
		return totalCountSql;
	}
	public void setTotalCountSql(String totalCountSql) {
		this.totalCountSql = totalCountSql;
	}
}
