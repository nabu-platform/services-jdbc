package be.nabu.libs.services.jdbc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import be.nabu.libs.property.ValueUtils;
import be.nabu.libs.property.api.Value;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.DefinedType;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.api.Type;
import be.nabu.libs.types.properties.CollectionNameProperty;
import be.nabu.libs.types.properties.ForeignKeyProperty;
import be.nabu.libs.types.properties.PrimaryKeyProperty;

public class JDBCUtils {
	private static void getFieldsInTable(ComplexType type, Map<String, Element<?>> children) {
		// we start with the local children
		for (Element<?> child : type) {
			if (!children.containsKey(child.getName())) {
				children.put(child.getName(), child);
			}
		}
		Type superType = type.getSuperType();
		// if we extend a complex type, check if it has its own collection name
		if (superType instanceof ComplexType) {
			String collectionName = ValueUtils.getValue(CollectionNameProperty.getInstance(), superType.getProperties());
			// if it doesn't, it is absorbed into the current table
			if (collectionName == null) {
				getFieldsInTable((ComplexType) superType, children);
			}
		}
	}
	public static List<Element<?>> getFieldsInTable(ComplexType type) {
		Map<String, Element<?>> children = new LinkedHashMap<String, Element<?>>();
		getFieldsInTable(type, children);
		return new ArrayList<Element<?>>(children.values());
	}
	
	public static List<String> getBinding(ComplexType from, ComplexType to) {
		if (!(to instanceof DefinedType)) {
			throw new IllegalStateException("Can only resolve the binding if the to type is defined");
		}
		Element<?> primary = null;
		for (Element<?> child : getFieldsInTable(to)) {
			Value<Boolean> property = child.getProperty(PrimaryKeyProperty.getInstance());
			if (property != null && property.getValue()) {
				primary = child;
				break;
			}
		}
		if (primary == null) {
			throw new IllegalStateException("Can not find the primary key in type: " + to);
		}
		Element<?> link = null;
		for (Element<?> child : getFieldsInTable(from)) {
			Value<String> property = child.getProperty(ForeignKeyProperty.getInstance());
			if (property != null) {
				String[] split = property.getValue().split(":");
				if (split[0].equals(((DefinedType) to).getId()) && (split.length == 1 || split[1].equals(primary.getName()))) {
					link = child;
					break;
				}
			}
		}
		if (link == null) {
			throw new IllegalStateException("Can not find foreign key from " + from + " to " + to);
		}
		return Arrays.asList(link.getName(), primary.getName());
	}
}
