package be.nabu.libs.services.jdbc.api;

import java.net.URI;
import java.sql.Types;
import java.util.Date;
import java.util.UUID;

import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;

public interface SQLDialect {
	public String rewrite(String sql, ComplexType input, ComplexType output);
	public String limit(String sql, Integer offset, Integer limit);
	public String buildCreateSQL(ComplexType type);
	public String buildInsertSQL(ComplexContent values);
	
	/**
	 * Contains the type mappings of which non-native type maps to which native type
	 * The conversion logic must be implemented in the converter framework
	 * The reverse conversion is done automatically by assigning it to the complex type
	 */
	public default Class<?> getTargetClass(Class<?> clazz) {
		return clazz != null && (URI.class.isAssignableFrom(clazz) || clazz.isEnum()) ? String.class : clazz;
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
}
