package be.nabu.libs.services.jdbc.api;

import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.libs.converter.ConverterFactory;
import be.nabu.libs.converter.api.Converter;
import be.nabu.libs.property.ValueUtils;
import be.nabu.libs.property.api.Value;
import be.nabu.libs.services.api.ServiceException;
import be.nabu.libs.types.CollectionHandlerFactory;
import be.nabu.libs.types.api.CollectionHandlerProvider;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.api.SimpleType;
import be.nabu.libs.types.api.Unmarshallable;
import be.nabu.libs.types.properties.ActualTypeProperty;
import be.nabu.libs.types.properties.CollectionNameProperty;
import be.nabu.libs.types.properties.FormatProperty;
import be.nabu.libs.types.properties.NameProperty;
import be.nabu.libs.types.properties.TimezoneProperty;
import be.nabu.libs.types.utils.DateUtils;
import be.nabu.libs.types.utils.DateUtils.Granularity;

public interface SQLDialect {
	
	/**
	 * Whether or not the database supports arrays as inputs
	 */
	public default boolean hasArraySupport(Element<?> element) {
		return !Date.class.isAssignableFrom(((SimpleType<?>) element.getType()).getInstanceClass());
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
	
	/**
	 * Build a "create table" sql statement from the given type
	 */
	public String buildCreateSQL(ComplexType type);
	
	/**
	 * Build an insert statement from the given values
	 */
	public String buildInsertSQL(ComplexContent values);
	
	/**
	 * Not all databases support all types of objects, for example postgres might support uuid and boolean natively, oracle might not
	 * This method allows you to cast types that are not supported to types that are
	 * The conversion logic must be implemented in the converter framework
	 * The reverse conversion is done automatically by assigning it to the complex content of the result
	 */
	public default Class<?> getTargetClass(Class<?> clazz) {
		return clazz != null && (URI.class.isAssignableFrom(clazz) || clazz.isEnum()) ? String.class : clazz;
	}
	
	public default Integer getSQLType(Element<?> element) {
		SimpleType<?> simpleType = (SimpleType<?>) element.getType();
		return getSQLType(simpleType.getInstanceClass());
	}
	
	public default Integer getSQLType(Class<?> instanceClass) {
		if (String.class.isAssignableFrom(instanceClass) || char[].class.isAssignableFrom(instanceClass) || URI.class.isAssignableFrom(instanceClass) || instanceClass.isEnum()) {
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
		else {
			return null;
		}
	}
	
	public default String getSQLName(Element<?> element) {
		SimpleType<?> simpleType = (SimpleType<?>) element.getType();
		return getSQLName(simpleType.getInstanceClass());
	}
	
	public default String getSQLName(Class<?> instanceClass) {
		if (String.class.isAssignableFrom(instanceClass) || char[].class.isAssignableFrom(instanceClass) || URI.class.isAssignableFrom(instanceClass) || UUID.class.isAssignableFrom(instanceClass) || instanceClass.isEnum()) {
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
	public default void setObject(PreparedStatement statement, Element<?> element, int index, Object value) throws SQLException, ServiceException {
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
					statement.setNull(index, Types.ARRAY, getSQLName(element));
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
				statement.setNull(index, getSQLType(element), sqlTypeName);
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
		return "select count(a.*) as total from (" + query + ") a";
	}
	
	public static String getName(Value<?>...properties) {
		String value = ValueUtils.getValue(CollectionNameProperty.getInstance(), properties);
		if (value == null) {
			value = ValueUtils.getValue(NameProperty.getInstance(), properties);
		}
		return value;
	}
}
