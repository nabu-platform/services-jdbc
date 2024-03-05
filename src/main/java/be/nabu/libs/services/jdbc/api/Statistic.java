package be.nabu.libs.services.jdbc.api;

// you can combine multiple fields with "," separation. you also get the name and value back in that way
public interface Statistic {
	// the name of the field(s)
	public String getName();
	// the value of the field(s)
	public String getValue();
	public Long getAmount();
}
