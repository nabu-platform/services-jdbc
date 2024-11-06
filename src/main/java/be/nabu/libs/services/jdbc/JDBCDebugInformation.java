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
