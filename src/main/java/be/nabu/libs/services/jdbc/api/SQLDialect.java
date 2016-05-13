package be.nabu.libs.services.jdbc.api;

import be.nabu.libs.types.api.ComplexType;

public interface SQLDialect {
	public String rewrite(String sql, ComplexType input, ComplexType output);
	public String limit(String sql, Integer offset, Integer limit);
	public String buildCreateSQL(ComplexType type);
}
