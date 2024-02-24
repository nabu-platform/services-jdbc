package be.nabu.libs.services.jdbc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import be.nabu.eai.api.NamingConvention;
import be.nabu.libs.property.ValueUtils;
import be.nabu.libs.property.api.Value;
import be.nabu.libs.types.DefinedTypeResolverFactory;
import be.nabu.libs.types.TypeUtils;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.DefinedType;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.api.Type;
import be.nabu.libs.types.base.TypeBaseUtils;
import be.nabu.libs.types.base.ValueImpl;
import be.nabu.libs.types.properties.CollectionNameProperty;
import be.nabu.libs.types.properties.DuplicateProperty;
import be.nabu.libs.types.properties.EnricherProperty;
import be.nabu.libs.types.properties.ForeignKeyProperty;
import be.nabu.libs.types.properties.ForeignNameProperty;
import be.nabu.libs.types.properties.HiddenProperty;
import be.nabu.libs.types.properties.PrimaryKeyProperty;
import be.nabu.libs.types.properties.RestrictProperty;
import be.nabu.libs.types.structure.Structure;

/*
 * Foreign names
 * the foreign name is meant to be in this format: <foreignKeyColumn>:<foreignTableColumnName>
 * so for example if type A has a field ownerId which points (via foreign key attribute) to B:id, type A also has a field "ownerName" which has foreign name "ownerId:name"
 * this means we will bind a.owner_id to b.id and retrieve b.name to put in A.ownerName
 * 
 * For insights a new addition was added, if you have know that a particular table (structure) is in the mix (e.g. insights aggregates over a table), 
 * you can reference fields that are _not_ in the type you are passing along as output but rather in another type.
 * e.g. customerId:naam@example.database.main.types.customerLine
 * Not sure if this is fully thought through, meaning, the insight will copy the collection name etc from the original core type, so it is addressing that table
 * That allows it to use other fields from that table that are not in the select itself.
 * However, I don't think at this point that you can reference "random" other tables, only another type that happens to already be in the join logic for other reasons? maybe even only the core table?
 * If not already bound, there would be no join binding for that table.
 * An alternative solution to this (can be changed later): insight can extend the core type and simply restrict everything.
 * That way I can resolve fields through the supertype (which is stable from a table perspective) rather than having this funky logic.
 * 
 * 
 * Dynamic foreign key problem
 * ----------------------------
 * If you reference a field that does not have a foreign key of itself, you should be able to give one along.
 * Why? Suppose you have the task framework which has a correlationId. That correlation id does not have a foreign key, cause from the framework perspective it can be anything.
 * At design time however you might have more information and provide "runtime" foreign keys, like workflow instance id, or node id.
 * The way to solve this is to extend Task, add a field for example workflowId. It can have a foreign key to the workflow table.
 * We then give the workflowId a foreignName of correlationId.
 * 
 * The foreign name logic will see that it has no subsequent part, so will just alias it to correlationId when selecting:
 * select t.correlation_id as workflow_id from tasks
 * 
 * If we use workflowId in the foreignName of another field, like workflowStateId = workflowId:stateId
 * Then the resolving will also see the alias and resolve it correctly.
 * 
 * We shouldn't allow multiple (dynamic) foreign keys to be added to correlationId because:
 * - this makes the foreignName syntax even more complex (the foreign keys would have to be in there to be applicable at any stage)
 * - there will be multiple bindings on the same field to different tables (correlation id to workflow it and to node id and...), so we would need to generate multiple table aliases.
 * - you can't differentiate between left outer join and inner join (based on multiplicity), you just have to hope correlationId is optional (it usually is in this case though)
 * 		- though it might be interesting explicitly force workflowId to be mandatory, this will force an inner join, giving you only the tasks that have a workflow attached to it...
 * 
 * A downside of this approach is that you do need to inject a workflowId field, this makes insights slightly harder as it can't actually have that, it would need another intermediary type.
 * 
 * The advantage of adding it to the foreign name however, is the ability to inject dynamic foreign keys at any path along the way.
 * This alias approach only allows dynamic foreign keys at 1 level. If you want to dynamically resolve workflowStateId, you can't inject a foreign key at that point.
 * 
 * workflowState = correlationId[nabu.misc.workflow.types.WorkflowInstance:id!]:stateId[nabu.misc.workflow.types.WorkflowState:id]:name
 * 
 * Use the exclamation mark to indicate that it must exist?
 * 
 * Normally we would generate the table name based on correlationId, but another binding might add
 * 
 * nodeName = correlationId[nabu.cms.core.types.model.core.Node:id]:name
 * 
 * If we can't support this approach with proper GUI, it might not be worth the trouble. At some point it's easier to just write custom SQL...
 */
public class JDBCUtils {
	
	public static List<ComplexType> getAllTypes(ComplexType type) {
		// we first build a list of all the types we need to add
		List<ComplexType> typesToAdd = new ArrayList<ComplexType>();
		// we assume our "outer" type always has its own table, if it extends other types, we need to dig deeper if parts of the values have to be stored in different tables
		// but at the very least the local extension fields will have to be in its own table
		typesToAdd.add(type);
		
		List<String> collectionNames = new ArrayList<String>();
		// we want to add the root collection
		String rootCollection = ValueUtils.getValue(CollectionNameProperty.getInstance(), type.getProperties());
		if (rootCollection == null) {
			rootCollection = NamingConvention.UNDERSCORE.apply(type.getName());
		}
		collectionNames.add(rootCollection);
		
		// we must keep a runny tally of restrictions because in an extension you can restrict something from another table
		// if you are restricting the same table, this works because we take the most specific definition
		// if we just keep the original type for a different table however, we lose this restriction information
		// this is why we create ad hoc extensions to do this
		List<String> restrictions = new ArrayList<String>();
		// we check supertypes that have specifically named tables that have not yet been included
		// child can use restrictions to restrict parent types, so the child is "correcter" in a given context
		while (type.getSuperType() != null) {
			// make sure we take into account restrictions we imposed on the supertype
			restrictions.addAll(TypeBaseUtils.getRestricted(type));
			Type superType = type.getSuperType();
			if (superType instanceof ComplexType) {
				String collectionName = ValueUtils.getValue(CollectionNameProperty.getInstance(), superType.getProperties());
				// we have part of the end result that resides in a dedicated table
				if (collectionName != null && collectionNames.indexOf(collectionName) < 0) {
					if (restrictions.isEmpty()) {
						typesToAdd.add((ComplexType) superType);
					}
					else {
						typesToAdd.add(restrict((ComplexType) superType, restrictions));
					}
					collectionNames.add(collectionName);
				}
				type = (ComplexType) superType;
			}
			// if we ever extend a simple type, we stop here
			else {
				break;
			}
		}
		return typesToAdd;
	}
	
	private static ComplexType restrict(ComplexType superType, List<String> restrictions) {
		Structure structure = new Structure();
		structure.setProperty(superType.getProperties());
		structure.setSuperType(superType);
		structure.setProperty(new ValueImpl<String>(RestrictProperty.getInstance(), join(restrictions)));
		// we add this so we can unwrap() it later
		structure.setProperty(new ValueImpl<Boolean>(HiddenProperty.getInstance(), true));
		return structure;
	}
	

	private static String join(List<String> restrictions) {
		StringBuilder builder = new StringBuilder();
		for (String restriction : restrictions) {
			if (!builder.toString().isEmpty()) {
				builder.append(",");
			}
			builder.append(restriction);
		}
		return builder.toString();
	}
	
	public static String getForeignNameTable(String foreignName) {
		int lastIndexOf = foreignName.indexOf('@');
		if (lastIndexOf >= 0) {
			foreignName = foreignName.substring(0, lastIndexOf);
		}
		
		String[] split = foreignName.split(":");
		// foreign key join
		// i don't want to use fk_ alone as it might coincide with other default naming
		StringBuilder builder = new StringBuilder("fkj_");
		// keep an eye on the max length (oracle!)
		builder.append(NamingConvention.UNDERSCORE.apply(split[0].length() <= 20 ? split[0] : split[0].substring(0, 20)));
		// the end result should be in the above example: "fkj_owner_id"
		// we don't want to also include the "b.name" field in it cause you might want to inject multiple values from the same binding
		return builder.toString();
	}
	
	public static List<String> getForeignNameFields(String foreignName) {
		int lastIndexOf = foreignName.indexOf('@');
		if (lastIndexOf >= 0) {
			foreignName = foreignName.substring(0, lastIndexOf);
		}
		
		return Arrays.asList(foreignName.split(":"));
	}
	
	public static ComplexType getForeignLookupType(String foreignName) {
		int lastIndexOf = foreignName.indexOf('@');
		if (lastIndexOf < 0) {
			return null;
		}
		return (ComplexType) DefinedTypeResolverFactory.getInstance().getResolver().resolve(foreignName.substring(lastIndexOf + 1));
	}
	
	public static List<String> getForeignNameTables(String foreignName) {
		int lastIndexOf = foreignName.indexOf('@');
		if (lastIndexOf >= 0) {
			foreignName = foreignName.substring(0, lastIndexOf);
		}
		
		lastIndexOf = foreignName.lastIndexOf(':');
		if (lastIndexOf < 0) {
			return null;
		}
		String bindings = foreignName.substring(0, lastIndexOf);
		
		// we need to stay under 30 characters (damn you oracle)
		// so we see how long the binding is and generate a name based on that
		
		String[] split = bindings.split(":");
		List<String> tables = new ArrayList<String>();
		// we want the first binding to be for example "fkj_owner_id", then "fkj_owne_something_else" etc etc
		// because we want to reuse the bindings as much as possible, irrespective of how many (or how deep) the bindings are
		// if you do another field that just uses fkj_owner_id, you want to reuse the binding, not cut it off because the initial binding was smaller
		for (int i = 0; i < split.length; i++) {
			// the minus i is for the underscores in between the parts
			int maxLengthForThisPart = (int) Math.floor((30 - i - "f_".length()) / (i + 1));
			maxLengthForThisPart -= i;
			// at least 2 though...
			if (maxLengthForThisPart < 2) {
				maxLengthForThisPart = 2;
			}
			String name = NamingConvention.UNDERSCORE.apply(split[i]);
			if (name.length() > maxLengthForThisPart) {
				name = name.substring(0, maxLengthForThisPart);
			}
			// don't end with underscore...
			if (name.endsWith("_")) {
				name = name.substring(0, name.length() - 1);
			}
			// we add the level to make sure it is unique
			// we had an issue where the "short" version of a table further down was the same as the normal version of the initial binding...
			StringBuilder builder = new StringBuilder("f" + i);
			for (int j = 0; j < i; j++) {
				String partName = NamingConvention.UNDERSCORE.apply(split[j]);
				if (partName.length() > maxLengthForThisPart) {
					partName = partName.substring(0, maxLengthForThisPart);
					// don't end with underscore...
					if (partName.endsWith("_")) {
						partName = partName.substring(0, partName.length() - 1);
					}
				}
				builder.append("_" + partName);
			}
			builder.append("_" + name);
			tables.add(builder.toString());
		}
		return tables;
	}
	private static boolean ignoreField(Element<?> element) {
		// a foreign name indicates an imported field which should not be persisted
		String foreignName = ValueUtils.getValue(ForeignNameProperty.getInstance(), element.getProperties());
		if (foreignName != null && !foreignName.trim().isEmpty()) {
			return true;
		}
		// the field is enriched from somewhere else
		String enricher = ValueUtils.getValue(EnricherProperty.getInstance(), element.getProperties());
		if (enricher != null && !enricher.trim().isEmpty()) {
			return true;
		}
		return false;
	}
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
				// make sure we don't have properties that make us ignore the field
				if (!ignoreField(child)) {
					children.put(child.getName(), child);
				}
			}
		}
		
		restrictions.addAll(TypeBaseUtils.getRestricted(type));
		
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
	public static String getTypeName(ComplexType type, boolean force) {
		String typeCollectionName = ValueUtils.getValue(CollectionNameProperty.getInstance(), type.getProperties());
		if (force && typeCollectionName == null) {
			// first we check in supertypes if we inherit a name
			Type superType = type.getSuperType();
			while (superType != null) {
				typeCollectionName = ValueUtils.getValue(CollectionNameProperty.getInstance(), superType.getProperties());
				if (typeCollectionName != null) {
					break;
				}
				superType = superType.getSuperType();
			}
			// if we can't find one, deduce from name
			if (typeCollectionName == null) {
				typeCollectionName = type.getName().replaceAll("([A-Z]+)", "_$1").replaceFirst("^_", "");
			}
		}
		return typeCollectionName;
	}
	public static List<Element<?>> getFieldsInTable(ComplexType type) {
		Map<String, Element<?>> children = new LinkedHashMap<String, Element<?>>();
		getFieldsInTable(type, children, true, new ArrayList<String>());
		return new ArrayList<Element<?>>(children.values());
	}
	
	public static List<String> getBinding(ComplexType from, ComplexType to) {
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
		
		// @2013-03-15: there are issues when one type extends the other AND it also has a foreign key to the other for different reasons
		// a primary example of this is masterdata: you might extend masterdata but also reference it, in this case the old code would use the foreign key binding to select instead of id-based
		// so we check if they are related to one another
		// we also assume that "from" extends "to" and not the other way around, not sure if we want to calculate bindings in that direction
		if (!TypeUtils.getUpcastPath(from, to).isEmpty()) {
			// if from extends to AND it has a duplicate property for the primary key we are using primary key-based inheritance (so the keys are kept in sync)
			// otherwise we might be using foreign key based extension at which point we want to fall back to the original logic
			String duplicateFrom = ValueUtils.getValue(DuplicateProperty.getInstance(), from.getProperties());
			// we have a duplicate property and it contains the primary key 
			if (duplicateFrom != null && Arrays.asList(duplicateFrom.split("[\\s]*,[\\s]*")).indexOf(primary.getName()) >= 0) {
				// we link from the same field to the same field
				return Arrays.asList(primary.getName(), primary.getName());
			}
		}
		
		
		Element<?> link = null;
		Element<?> childPrimary = null;
		for (Element<?> child : getFieldsInTable(from)) {
			Value<String> foreignKeyProperty = child.getProperty(ForeignKeyProperty.getInstance());
			if (foreignKeyProperty != null) {
				String[] split = foreignKeyProperty.getValue().split(":");
				if (to instanceof DefinedType && split[0].equals(((DefinedType) to).getId()) && (split.length == 1 || split[1].equals(primary.getName()))) {
					link = child;
				}
			}
			Value<Boolean> primaryProperty = child.getProperty(PrimaryKeyProperty.getInstance());
			if (primaryProperty != null && primaryProperty.getValue()) {
				childPrimary = child;
			}
		}
		// if we have no direct link but the primary id of the from and the primary of the to are the _exact_ same (even in memory) item
		// that means the child inherits it from the parent via a specific "duplicate" property
		// at that point (partially for backwards compatibility with uml) we assume the primary keys are linked to one another (even if not explicitly with a foreign key)
		// @2020-09-17: we become less strict in the exact same requirement, just having the same type and name is considered enough atm
		if (link == null && childPrimary != null && childPrimary.getName().equals(primary.getName()) && childPrimary.getType().equals(primary.getType())) {
			link = childPrimary;
		}
		if (link == null) {
			throw new IllegalStateException("Can not find foreign key from " + from + " to " + to);
		}
		return Arrays.asList(link.getName(), primary.getName());
	}
}
