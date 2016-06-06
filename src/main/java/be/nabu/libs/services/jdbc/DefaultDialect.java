package be.nabu.libs.services.jdbc;

import be.nabu.libs.services.jdbc.api.SQLDialect;
import be.nabu.libs.types.api.ComplexType;

public class DefaultDialect implements SQLDialect {
	@Override
	public String rewrite(String sql, ComplexType input, ComplexType output) {
		return sql;
	}
	@Override
	public String limit(String sql, Integer offset, Integer limit) {
		return sql;
	}
	@Override
	public String buildCreateSQL(ComplexType type) {
		return null;
	}
}
