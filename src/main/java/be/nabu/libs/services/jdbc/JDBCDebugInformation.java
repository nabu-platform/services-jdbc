package be.nabu.libs.services.jdbc;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "jdbc")
public class JDBCDebugInformation {
	
	private String connectionId, transactionId, sql, totalCountSql, serviceContext, statisticsSql;
	private Integer inputAmount, outputAmount;
	private Long executionDuration, mappingDuration, totalCountDuration, statisticsDuration, statisticsMappingDuration;
	
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
	public Long getTotalCountDuration() {
		return totalCountDuration;
	}
	public void setTotalCountDuration(Long totalCountDuration) {
		this.totalCountDuration = totalCountDuration;
	}
	public String getServiceContext() {
		return serviceContext;
	}
	public void setServiceContext(String serviceContext) {
		this.serviceContext = serviceContext;
	}
	public Long getStatisticsDuration() {
		return statisticsDuration;
	}
	public void setStatisticsDuration(Long statisticsDuration) {
		this.statisticsDuration = statisticsDuration;
	}
	public String getStatisticsSql() {
		return statisticsSql;
	}
	public void setStatisticsSql(String statisticsSql) {
		this.statisticsSql = statisticsSql;
	}
	public Long getStatisticsMappingDuration() {
		return statisticsMappingDuration;
	}
	public void setStatisticsMappingDuration(Long statisticsMappingDuration) {
		this.statisticsMappingDuration = statisticsMappingDuration;
	}
}
