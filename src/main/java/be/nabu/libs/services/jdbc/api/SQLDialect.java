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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.libs.converter.ConverterFactory;
import be.nabu.libs.converter.api.Converter;
import be.nabu.libs.property.ValueUtils;
import be.nabu.libs.property.api.Value;
import be.nabu.libs.services.api.ServiceException;
import be.nabu.libs.services.jdbc.JDBCService;
import be.nabu.libs.types.CollectionHandlerFactory;
import be.nabu.libs.types.api.CollectionHandlerProvider;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.api.SimpleType;
import be.nabu.libs.types.api.Unmarshallable;
import be.nabu.libs.types.base.Duration;
import be.nabu.libs.types.properties.ActualTypeProperty;
import be.nabu.libs.types.properties.CollectionNameProperty;
import be.nabu.libs.types.properties.FormatProperty;
import be.nabu.libs.types.properties.NameProperty;
import be.nabu.libs.types.properties.TimezoneProperty;
import be.nabu.libs.types.utils.DateUtils;
import be.nabu.libs.types.utils.DateUtils.Granularity;

public interface SQLDialect {

	// to be expanded upon...
	public static List<String> sqlReservedWords = Arrays.asList("or", "and", "from", "select", "join", "on", "is", "as", "to");
	
	/**
	 * Whether or not the database supports arrays as inputs
	 */
	public default boolean hasArraySupport(Element<?> element) {
		return !Date.class.isAssignableFrom(((SimpleType<?>) element.getType()).getInstanceClass());
	}
	
	/**
	 * Some databases expect the table name to always be uppercase, even if the original create was lowercase (oracle)
	 * Some databases expect the table name to be the same as when it was created (postgresql)
	 */
	public default String standardizeTablePattern(String tableName) {
		return tableName;
	}
	
	public default boolean supportNumericGroupBy() {
		return false;
	}
	
	/**
	 * Rewrite an sql statement where necessary
	 */
	public String rewrite(String sql, ComplexType input, ComplexType output);
	
	/**
	 * Add an offset and a limit to an sql statement using database-specific syntax
	 * If you return null, it is assumed you can't and application level limiting will be applied
	 */
	public String limit(String sql, Long offset, Integer limit);
	
	public default String buildDropSQL(ComplexType type, String element) {
		String name = getName(type.getProperties()).replaceAll("([A-Z]+)", "_$1").replaceFirst("^_", "");
		name = quoteReserved(name.toLowerCase());
		return "alter table " + name + " drop " + element.replaceAll("([A-Z]+)", "_$1").replaceFirst("^_", "") + ";";
	}
	
	/**
	 * By default we try to extract it from the create
	 */
	public default String buildAlterSQL(ComplexType type, String element) {
		String name = getName(type.getProperties()).replaceAll("([A-Z]+)", "_$1").replaceFirst("^_", "");
		name = quoteReserved(name.toLowerCase());
		return "alter table " + name + " add " + guessAlter(type, element) + ";";
	}
	
	public default String buildAlterNillable(ComplexType type, String element, boolean nillable) {
		String tableName = getName(type.getProperties()).replaceAll("([A-Z]+)", "_$1").replaceFirst("^_", "");
		tableName = quoteReserved(tableName.toLowerCase());
		String columnName = element.replaceAll("([A-Z]+)", "_$1").replaceFirst("^_", "").toLowerCase();
		columnName = quoteReserved(columnName);
		// this is the default syntax that works on postgres & h2 and likely some others...
		return "alter table " + tableName + " alter " + columnName + " " + (nillable ? "drop" : "set") + " not null;";
	}
	
	public default String guessAlter(ComplexType type, String element) {
		String create = this.buildCreateSQL(type, false);
		String columnName = element.replaceAll("([A-Z]+)", "_$1").replaceFirst("^_", "").toLowerCase();
		String[] split = create.toLowerCase().split("\\b" + columnName + "\\b");
		columnName = quoteReserved(columnName);
		// it can be greater than 2, for instance if there is a foreign key definition at the end which includes the column name
		// in all cases, the correct name should be in split[1]
		if (split.length < 2) {
			throw new IllegalArgumentException("Could not find alter for '" + element + "' (" + columnName + ") in: " + create);
		}
		String trimmed = split[1].trim();
		if (trimmed.startsWith("\"")) {
			trimmed = trimmed.substring(1).trim();
		}
		String alter = columnName + " " + trimmed;
		int depth = 0;
		for (int i = 0; i < alter.length(); i++) {
			if (alter.charAt(i) == '(') {
				depth++;
			}
			else if (alter.charAt(i) == ')') {
				// the end has been reached
				if (depth == 0) {
					return alter.substring(0, i).trim();
				}
				depth--;
			}
			else if (alter.charAt(i) == ',') {
				// the end has been reached
				if (depth == 0) {
					return alter.substring(0, i).trim();
				}
			}
		}
//		String alter = create.replaceAll("(?s)(?i).*?(\\b" + element.replaceAll("([A-Z]+)", "_$1").replaceFirst("^_", "") + "\\b[^,]+?)(?:,|[\\s]*\\);).*", "$1");
//		if (alter.equals(create)) {
//			throw new IllegalArgumentException("Could not find alter for: " + element);
//		}
		// don't start with a ", this means quoted strings
		return alter;
	}
	
	/**
	 * Build a "create table" sql statement from the given type
	 */
	public default String buildCreateSQL(ComplexType type) {
		return this.buildCreateSQL(type, false);
	}
	
	public String buildCreateSQL(ComplexType type, boolean compact);
	/**
	 * Build an insert statement from the given values
	 */
	public default String buildInsertSQL(ComplexContent values) {
		return this.buildInsertSQL(values, false);
	}
	
	public String buildInsertSQL(ComplexContent values, boolean compact);
	
	/**
	 * Not all databases support all types of objects, for example postgres might support uuid and boolean natively, oracle might not
	 * This method allows you to cast types that are not supported to types that are
	 * The conversion logic must be implemented in the converter framework
	 * The reverse conversion is done automatically by assigning it to the complex content of the result
	 */
	public default Class<?> getTargetClass(Class<?> clazz) {
		return clazz != null && (URI.class.isAssignableFrom(clazz) || clazz.isEnum() || Duration.class.isAssignableFrom(clazz)) ? String.class : clazz;
	}
	
	public default Integer getSQLType(Element<?> element) {
		SimpleType<?> simpleType = (SimpleType<?>) element.getType();
		return getSQLType(simpleType.getInstanceClass());
	}
	
	public default Integer getSQLType(Class<?> instanceClass) {
		if (String.class.isAssignableFrom(instanceClass) || char[].class.isAssignableFrom(instanceClass) || URI.class.isAssignableFrom(instanceClass) || instanceClass.isEnum() || Duration.class.isAssignableFrom(instanceClass)) {
			return Types.VARCHAR;
		}
		else if (byte[].class.isAssignableFrom(instanceClass)) {
			return Types.VARBINARY;
		}
		else if (Integer.class.isAssignableFrom(instanceClass)) {
			return Types.INTEGER;
		}
		else if (Long.class.isAssignableFrom(instanceClass)) {
			return Types.BIGINT;
		}
		else if (Double.class.isAssignableFrom(instanceClass)) {
			return Types.DOUBLE;
		}
		else if (Float.class.isAssignableFrom(instanceClass)) {
			return Types.FLOAT;
		}
		else if (Short.class.isAssignableFrom(instanceClass)) {
			return Types.SMALLINT;
		}
		else if (Boolean.class.isAssignableFrom(instanceClass)) {
			return Types.BOOLEAN;
		}
		else if (UUID.class.isAssignableFrom(instanceClass)) {
			return Types.OTHER;
		}
		else if (Date.class.isAssignableFrom(instanceClass)) {
			return Types.TIMESTAMP;
		}
		else if (BigDecimal.class.isAssignableFrom(instanceClass)) {
			return Types.DECIMAL;
		}
		else if (BigInteger.class.isAssignableFrom(instanceClass)) {
			return Types.NUMERIC;
		}
		else {
			return null;
		}
	}
	
	/**
	 * The name of the element when the value is null
	 */
	public default String getSQLNullName(Element<?> element) {
		return getSQLName(element);
	}
	
	public default String getSQLName(Element<?> element) {
		SimpleType<?> simpleType = (SimpleType<?>) element.getType();
		return getSQLName(simpleType.getInstanceClass());
	}
	
	public default String getSQLName(Class<?> instanceClass) {
		if (String.class.isAssignableFrom(instanceClass) || char[].class.isAssignableFrom(instanceClass) || URI.class.isAssignableFrom(instanceClass) || UUID.class.isAssignableFrom(instanceClass) || instanceClass.isEnum() || Duration.class.isAssignableFrom(instanceClass)) {
			return "varchar";
		}
		else if (byte[].class.isAssignableFrom(instanceClass)) {
			return "varbinary";
		}
		else if (Integer.class.isAssignableFrom(instanceClass)) {
			return "integer";
		}
		else if (Long.class.isAssignableFrom(instanceClass)) {
			return "bigint";
		}
		else if (Double.class.isAssignableFrom(instanceClass)) {
			return "double";
		}
		else if (Float.class.isAssignableFrom(instanceClass)) {
			return "float";
		}
		else if (Short.class.isAssignableFrom(instanceClass)) {
			return "smallint";
		}
		else if (Boolean.class.isAssignableFrom(instanceClass)) {
			return "boolean";
		}
		else if (Date.class.isAssignableFrom(instanceClass)) {
			return "timestamp";
		}
		else {
			return null;
		}
	}
	
	// this method sets the content of an element to the statement
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public default void setObject(PreparedStatement statement, Element<?> element, int index, Object value, String originalQuery) throws SQLException, ServiceException {
		Logger logger = LoggerFactory.getLogger(getClass());
		Converter converter = ConverterFactory.getInstance().getConverter();

		SimpleType<?> simpleType = (SimpleType<?>) element.getType();
		logger.trace("Setting parameter '{}' = '{}'", element.getName(), value);
		// if it's a string, let's check if it has an actual type
		// if so, convert it first
		if (String.class.isAssignableFrom(simpleType.getInstanceClass())) {
			SimpleType<?> actualType = ValueUtils.getValue(ActualTypeProperty.getInstance(), element.getProperties());
			if (actualType != null) {
				simpleType = actualType;
				if (actualType instanceof Unmarshallable && value instanceof String) {
					value = ((Unmarshallable<?>) actualType).unmarshal((String) value, element.getProperties());
				}
			}
		}
		// TODO: dates can NOT be set as a list because in the current solution we use setObject() and an array, however in this case it is impossible to actually set a calendar! As a workaround format to string if needed
		// if it's a date, check the parameters for timezone etc
		if (Date.class.isAssignableFrom(simpleType.getInstanceClass())) {
			String format = ValueUtils.getValue(FormatProperty.getInstance(), element.getProperties());
			Granularity granularity = format == null ? Granularity.TIMESTAMP : DateUtils.getGranularity(format);
			TimeZone timezone = ValueUtils.getValue(TimezoneProperty.getInstance(), element.getProperties());
			Calendar calendar = Calendar.getInstance(timezone);
			switch(granularity) {
				case DATE:
					if (value == null) {
						statement.setNull(index, Types.DATE);
					}
					else {
						statement.setDate(index, new java.sql.Date(((Date) value).getTime()), calendar);
					}
				break;
				case TIME:
					if (value == null) {
						statement.setNull(index, Types.TIME);	
					}
					else {
						statement.setTime(index, new java.sql.Time(((Date) value).getTime()), calendar);
					}
				break;
				case TIMESTAMP:
					if (value == null) {
						statement.setNull(index, Types.TIMESTAMP);
					}
					else {
						statement.setTimestamp(index, new java.sql.Timestamp(((Date) value).getTime()), calendar);
					}
				break;
				default:
					throw new ServiceException("JDBC-4", "Unknown date granularity: " + granularity, granularity);
			}
		}
		else {
			// check if there is some known conversion logic
			// anything else is given straight to the JDBC adapter in the hopes that it can handle the type
			boolean isList = element.getType().isList(element.getProperties());
			if (value != null) {
				// IMPORTANT: this bit of code assumes the database supports the setting of an object array instead of a single value
				// some databases support this, some don't so it depends on the database and more specifically its driver
				if (isList && hasArraySupport(element)) {
					CollectionHandlerProvider provider = CollectionHandlerFactory.getInstance().getHandler().getHandler(value.getClass());
					if (provider == null) {
						throw new RuntimeException("Unknown collection type: " + value.getClass());
					}
					setArray(statement, element, index, provider.getAsCollection(value));
				}
				else {
					Class<?> targetType = getTargetClass(value.getClass());
					if (!targetType.equals(value.getClass())) {
						value = converter.convert(value, targetType);
						if (value == null) {
							throw new RuntimeException("Could not convert the value to: " + targetType);
						}
					}
					statement.setObject(index, value);
				}
			}
			else {
				Integer sqlType = getSQLType(simpleType.getInstanceClass());
				// could not perform a mapping, just pass it to the driver and hope it can figure it out
				if (sqlType == null) {
					logger.warn("Could not map instance class to native SQL type: {}", simpleType.getInstanceClass());
					statement.setObject(index, null);	
				}
				else if (isList && hasArraySupport(element)) {
					// don't set empty array as you can no longer do "is null"
//					java.sql.Array array = statement.getConnection().createArrayOf(getSQLName(simpleType.getInstanceClass()), new Object[0]);
//					statement.setObject(index, array, Types.ARRAY);
//					statement.setObject(index, null, Types.ARRAY);
					statement.setNull(index, Types.ARRAY, getSQLNullName(element));
				}
				else {
					statement.setNull(index, sqlType);
				}
			}
		}
	}
	
	public default void setArray(PreparedStatement statement, Element<?> element, int index, Collection<?> collection) throws SQLException {
		String sqlTypeName = getSQLName(element);
		// most databases that do support object arrays don't support untyped object arrays, so if we can't deduce the sql type, this will very likely fail
		if (sqlTypeName == null) {
			Logger logger = LoggerFactory.getLogger(getClass());
			logger.warn("Could not map instance class to native SQL type: {}", element.getName());
			if (collection.isEmpty()) {
				statement.setObject(index, null);		
			}
			else {
				statement.setObject(index, collection.toArray());
			}
		}
		else {
			if (collection.isEmpty()) {
				statement.setNull(index, Types.ARRAY, getSQLNullName(element));
//				statement.setNull(index, getSQLType(element), sqlTypeName);
			}
			else {
				// we can either create an array that is specific to the database
				// note: even using this construct you can not do "where a in (?)" but instead need to do "where a = any(?)" (at least for postgresql)
				java.sql.Array array = statement.getConnection().createArrayOf(sqlTypeName, collection.toArray());
				statement.setObject(index, array, Types.ARRAY);
				
				// or simply pass an object array and hope it works...
				// note: this does NOT work on postgresql
//								Object first = collection.iterator().next();
//								Object newInstance = Array.newInstance(first.getClass(), collection.size());
//								statement.setObject(index++, collection.toArray((Object[]) newInstance), sqlType);
			}
		}
	}
	
	public default String getTotalCountQuery(String query) {
		return getDefaultTotalCountQuery(query);
	}

	public static String getDefaultTotalCountQuery(String query) {
		return getDefaultTotalCountQuery(query, false);
	}
	
	// TODO: the query handling is very lightweight currently
	/*
	 * Note: we know that a field can be selected multiple times
	 * For instance you could want statistics for:
	 * batteryTypeId,chemicalFamilyId
	 * chemicalFamilyId
	 * This means you want the chemical family both separately and in the grouping with battery type id
	 * We "could" optimize the select to only select the chemical family once, but it is also OK to select it multiple times
	 * By selecting it multiple times, it's easier to jump to the correct position to fetch the value rather than having to know the other groups
	 * This decision is obviously tightly coupled to the jdbc service instance running the actual query
	 * 
	 * Manual example query:
			select grouping(rechargeable), rechargeable, battery_usage_id, chemical_family_id, battery_usage_id, battery_category_id, count(*) from battery_types where chemical_family_id='2bbd25d1e8c846a98bf1f2cb698712df' group by grouping sets(
				(rechargeable),
				(battery_usage_id),
				(battery_category_id, chemical_family_id)
			)
			
		Generated example:
		
		select
			grouping(ticker_id),
			grouping(ticker_eod_import_id),
			ticker_id,
			ticker_eod_import_id,
			count(*)
		from  ticker_eods
		group by grouping sets((ticker_id),
			(ticker_eod_import_id))
	 */
	public static String getDefaultStatisticsQuery(String query, List<String> statistics) {
		// we don't do specials just yet, focus on simple stuff
		if (isGrouped(query) || isUnion(query) || isDistinct(query)) {
			return null;
		}
		String groupBy = "";
		String grouping = "";
		String fields = "";
		for (String statistic : statistics) {
			if (statistic == null) {
				continue;
			}
			if (!grouping.isEmpty()) {
				grouping += ",\n\t";
				fields += ",\n\t";
				groupBy += ",\n\t";
			}
			grouping += "grouping(" + statistic + ")";
			
			fields += statistic;
			
			groupBy += "(" + statistic + ")";
		}
		String select = "select\n\t" + grouping + ",\n\t" + fields + ",\n\tcount(*)";
		String fromPart = query.split("(?i)(?s)\\bfrom\\b", 2)[1];
		String beforeGroupBy = fromPart.split("(?i)(?s)\\bgroup by\\b")[0];
		String result = select + "\nfrom " + beforeGroupBy + "\ngroup by grouping sets(" + groupBy + ")";
		return result;
	}
	
	public static String getDefaultTotalCountQuery(String query, boolean useStarCount) {
		// if the query is grouped, we can't do the optimized count as it will count within the grouping rules
		// unions are similarly tricky to rewrite
		if (isGrouped(query) || isUnion(query) || isDistinct(query)) {
//			return "select count(a.*) as total from (" + query + ") a";
			// postgresq, h2 etc don't seem to mind the count(a.*) but oracle doesn't like it. there seems to be no harm in leaving it out
			return "select count(*) as total from (" + query + ") a";
		}
		// otherwise we optimize the count query which can result in drastic performance increases
		else {
			// first we remove any order by statements, they can be massive performance drains and are not necessary to do a count
			String[] split = query.split("(?i)order by");
			StringBuilder builder = new StringBuilder();
			for (int i = 0; i < split.length; i++) {
				// the first part is the beginning of the query, just append it whole
				if (i == 0) {
					builder.append(split[i]);
				}
				// we are not interested in the order by but we might be interested in what comes _after_ the order by (if anything)
				// all the other parts follow an "order by statement"
				else {
					String content = split[i].trim();
					boolean first = true;
					// we want to strip any and all fields that are ordered by
					// it can be sql-dialect specific which keywords can follow an "order by" so slightly harder to strip correctly based on what comes next
					// instead we assume all field names are \w and optionally a . for table name
					// we strip one field name, then we check if the first character after that is a ",", if so we are ordering by multiple fields
					// note that we also check for optional asc or desc
					while (first || content.startsWith(",") || content.startsWith("(")) {
						if (first) {
							first = false;
							content = content.replaceFirst("^[\\w.]+", "").trim();
						}
						// skip the comma
						else if (content.startsWith(",")) {
							content = content.substring(1);
							content = content.trim();
							content = content.replaceFirst("^[\\w.]+", "").trim();
						}
						// we have a leading (, check where it ends
						else {
							int depth = 0;
							int until = -1;
							for (int j = 0; j < content.length(); j++) {
								if (content.charAt(j) == '(') {
									depth++;
								}
								else if (content.charAt(j) == ')') {
									depth--;
								}
								if (depth == 0) {
									until = j;
									break;
								}
							}
							if (until < 0) {
								throw new IllegalArgumentException("Invalid depth");
							}
							content = content.substring(until + 1);
						}
						if (content.length() >= 3 && content.substring(0, 3).equalsIgnoreCase("asc")) {
							content = content.substring(3);
						}
						if (content.length() >= 4 && content.substring(0, 4).equalsIgnoreCase("desc")) {
							content = content.substring(4);
						}
					}
					builder.append(" ").append(content).append(" ");
				}
			}
			query = builder.toString();
			// then we want to update the select statement and only select a count of the first field in the select
			
			builder = new StringBuilder();
			Integer index = null;
			while (index == null || index >= 0) {
				index = query.toLowerCase().indexOf("select", index == null ? -1 : index + 1);
				if (index >= 0) {
					String first = query.substring(0, index);
					// @2025-02-24: someone was writing comments like this: -- 1) ...
					// this falsely triggered the bracket count in test query 2 for the subselect "select 1 from unnest..."
					String firstNoComments = first.replaceAll("(?m)--.*$", "");
					int depth = (firstNoComments.length() - firstNoComments.replace("(", "").length()) - (firstNoComments.length() - firstNoComments.replace(")", "").length());
					// if we are at depth 0, we are in the core select, we want to rewrite that
					if (depth == 0) {
						String second = query.substring(index);
						// @2025-02-11: when you have a depth 0 reference to the word select (e.G. it is part of a field name like "age_selection", it will replace that as well...
						if (!second.matches("(?i)(?s)^select\\b.*")) {
							continue;
						}
						// we append the begin part, whatever it may be (e.g. a with)
						builder.append(first);
						String[] split2 = second.toLowerCase().split("(?i)[\\s]*\\bfrom\\b");
//						int from = second.toLowerCase().indexOf("from");
						if (split2.length < 2) {
							throw new IllegalStateException("No from found");
						}
						int from = split2[0].length();
						String selected = null;
						String [] selectString = second.substring("select".length(), from).split(",");

						depth = 0;
						for (String possible : selectString) {
							// ugly but we want to get methods as a whole, so we don't accidently use a comma that is part of a nested method call
							depth += (possible.length() - possible.replace("(", "").length()) - (possible.length() - possible.replace(")", "").length());
							if (depth != 0) {
								if (selected == null) {
									selected = possible;
								}
								else {
									selected += possible;
								}
							}
							else {
								possible = possible.trim();
								if (possible.matches(".*\\bas\\b.*")) {
									possible = possible.replaceFirst("^(.*?)\\bas\\b.*", "$1");
								}
								// we can't do a count on null, it always ends in 0 apparently!
								if (!possible.equalsIgnoreCase("null")) {
									if (selected == null) {
										selected = possible;
									}
									else {
										selected += possible;
									}
								}
							}
							// we have a full selection
							if (selected != null && depth == 0) {
								break;
							}
						}
						if (selected == null) {
							throw new IllegalArgumentException("Could not find a selectable field in: " + second.substring("select".length(), from));
						}
						selected = selected.trim();
						builder.append("select count(").append(useStarCount ? "*" : selected).append(") as total ").append(second.substring(from));
					}
				}
			}
			query = builder.toString();
			return query;
		}
//		return "select count(a.*) as total from (" + query + ") a";
	}
	
	public static boolean isDistinct(String sql) {
		if (sql.matches("(?i)(?s)^[\\s]*select[\\s]+distinct[\\s]+.*")) {
			return true;
		}
		return false;
	}
	
	public static boolean isGrouped(String sql) {
		Integer index = null;
		while (index == null || index >= 0) {
			index = sql.toLowerCase().indexOf("group by", index == null ? -1 : index + 1);
			if (index >= 0) {
				String first = sql.substring(0, index);
				int depth = (first.length() - first.replace("(", "").length()) - (first.length() - first.replace(")", "").length());
				if (depth == 0) {
					return true;
				}
			}
		}
		return false;
	}
	
	public static boolean isUnion(String sql) {
		return sql.matches("(?i)(?s).*\\bunion\\b.*");
	}
	
	public static String getName(Value<?>...properties) {
		String value = ValueUtils.getValue(CollectionNameProperty.getInstance(), properties);
		if (value == null) {
			value = ValueUtils.getValue(NameProperty.getInstance(), properties);
		}
		return value;
	}
	
	public default Integer getDefaultPort() {
		return null;
	}
	
	public default List<String> getReservedWords() {
		return new ArrayList<String>();
	}
	
	public default String quoteReserved(String name) {
		for (String reserved : getReservedWords()) {
			if (name.equalsIgnoreCase(reserved)) {
				return "\"" + name + "\"";
			}
		}
		return name;
	}
	
	// wrap it in something sensible if possible
	public default Exception wrapException(SQLException e) {
		return null;
	}
	
	public default String getEstimateTotalCountQuery(String query) {
		return "explain " + query;
	}
	
	public default Long getEstimateTotalCount(ResultSet totalCount) throws SQLException {
		String explainResult = totalCount.getString(1);
		System.out.println("xplain result is: " + explainResult);
		return Long.parseLong(explainResult.replaceAll(".*?rows=([0-9]+).*", "$1"));
	}
}
