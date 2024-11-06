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

import be.nabu.libs.services.jdbc.api.SQLDialect;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;

public class DefaultDialect implements SQLDialect {
	@Override
	public String rewrite(String sql, ComplexType input, ComplexType output) {
		return sql;
	}
	@Override
	public String limit(String sql, Long offset, Integer limit) {
		return sql;
	}
	@Override
	public String buildCreateSQL(ComplexType type, boolean compact) {
		return null;
	}
	@Override
	public String buildInsertSQL(ComplexContent values, boolean compact) {
		return null;
	}
}
