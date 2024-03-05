package be.nabu.libs.services.jdbc;

import be.nabu.libs.services.jdbc.api.Statistic;

public class StatisticImpl implements Statistic {
	private String name, value;
	private Long amount;
	@Override
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	@Override
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	@Override
	public Long getAmount() {
		return amount;
	}
	public void setAmount(Long amount) {
		this.amount = amount;
	}
}
