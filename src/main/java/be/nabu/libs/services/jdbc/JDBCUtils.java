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
import be.nabu.libs.types.properties.DuplicateProperty;
import be.nabu.libs.types.properties.ForeignKeyProperty;
import be.nabu.libs.types.properties.PrimaryKeyProperty;
import be.nabu.libs.types.properties.RestrictProperty;

public class JDBCUtils {
	private static void getFieldsInTable(ComplexType type, Map<String, Element<?>> children, boolean isRoot, List<String> restrictions) {
		String typeCollectionName = getTypeName(type, isRoot);
		
		// we add the fields we explicitly tagged as inherited
		String duplicated = ValueUtils.getValue(DuplicateProperty.getInstance(), type.getProperties());
		if (duplicated != null && !duplicated.trim().isEmpty()) {
			for (String single : duplicated.split("[\\s]*,[\\s]*")) {
				Element<?> child = type.get(single);
				if (child != null && !restrictions.contains(single)) {
					children.put(single, child);
				}
			}
		}
		
		// we add the local children that were not restricted from above
		for (Element<?> child : type) {
			if (!children.containsKey(child.getName()) && !restrictions.contains(child.getName())) {
				children.put(child.getName(), child);
			}
		}
		
		String restriction = ValueUtils.getValue(RestrictProperty.getInstance(), type.getProperties());
		if (restriction != null) {
			restrictions.addAll(Arrays.asList(restriction.split("[\\s]*,[\\s]*")));
		}
		
		Type superType = type.getSuperType();
		// if we extend a complex type, check if it has its own collection name
		if (superType instanceof ComplexType) {
			String collectionName = ValueUtils.getValue(CollectionNameProperty.getInstance(), superType.getProperties());
			// if it doesn't or it is the same table, it is absorbed into the current table
			if (collectionName == null || collectionName.equalsIgnoreCase(typeCollectionName)) {
				getFieldsInTable((ComplexType) superType, children, false, restrictions);
			}
		}
	}
	private static String getTypeName(ComplexType type, boolean force) {
		String typeCollectionName = ValueUtils.getValue(CollectionNameProperty.getInstance(), type.getProperties());
		if (force && typeCollectionName == null) {
			typeCollectionName = type.getName().replaceAll("([A-Z]+)", "_$1").replaceFirst("^_", "");
		}
		return typeCollectionName;
	}
	public static List<Element<?>> getFieldsInTable(ComplexType type) {
		Map<String, Element<?>> children = new LinkedHashMap<String, Element<?>>();
		getFieldsInTable(type, children, true, new ArrayList<String>());
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
		Element<?> childPrimary = null;
		for (Element<?> child : getFieldsInTable(from)) {
			Value<String> foreignKeyProperty = child.getProperty(ForeignKeyProperty.getInstance());
			if (foreignKeyProperty != null) {
				String[] split = foreignKeyProperty.getValue().split(":");
				if (split[0].equals(((DefinedType) to).getId()) && (split.length == 1 || split[1].equals(primary.getName()))) {
					link = child;
				}
			}
			Value<Boolean> primaryProperty = child.getProperty(PrimaryKeyProperty.getInstance());
			if (primaryProperty != null && primaryProperty.getValue()) {
				childPrimary = child;
			}
		}
		// if we have no direct link but the primary of the from and the primary of the to are the _exact_ same (even in memory) item
		// that means the child inherits it from the parent via a specific "duplicate" property
		// at that point (partially for backwards compatibility with uml) we assume the primary keys are linked to one another (even if not explicitly with a foreign key)
		if (link == null && childPrimary != null && childPrimary.equals(primary)) {
			link = childPrimary;
		}
		if (link == null) {
			throw new IllegalStateException("Can not find foreign key from " + from + " to " + to);
		}
		return Arrays.asList(link.getName(), primary.getName());
	}
}
