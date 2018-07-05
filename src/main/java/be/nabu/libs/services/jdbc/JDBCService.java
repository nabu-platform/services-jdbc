package be.nabu.libs.services.jdbc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import be.nabu.libs.property.ValueUtils;
import be.nabu.libs.property.api.Value;
import be.nabu.libs.services.api.DefinedService;
import be.nabu.libs.services.api.ServiceInstance;
import be.nabu.libs.services.api.ServiceInterface;
import be.nabu.libs.services.jdbc.api.ChangeTracker;
import be.nabu.libs.services.jdbc.api.DynamicDataSourceResolver;
import be.nabu.libs.services.jdbc.api.SQLDialect;
import be.nabu.libs.types.SimpleTypeWrapperFactory;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.api.KeyValuePair;
import be.nabu.libs.types.api.ModifiableComplexType;
import be.nabu.libs.types.api.SimpleTypeWrapper;
import be.nabu.libs.types.base.ComplexElementImpl;
import be.nabu.libs.types.base.SimpleElementImpl;
import be.nabu.libs.types.base.ValueImpl;
import be.nabu.libs.types.java.BeanResolver;
import be.nabu.libs.types.properties.CollectionNameProperty;
import be.nabu.libs.types.properties.CommentProperty;
import be.nabu.libs.types.properties.HiddenProperty;
import be.nabu.libs.types.properties.MaxOccursProperty;
import be.nabu.libs.types.properties.MinOccursProperty;
import be.nabu.libs.types.properties.NameProperty;
import be.nabu.libs.types.structure.DefinedStructure;
import be.nabu.libs.types.structure.Structure;
import be.nabu.libs.validator.api.ValidationMessage;

/**
 * TODO: allow setting of schema (e.g. https://github.com/WASdev/standards.jsr352.jbatch/blob/c55cc5ee9676f8da0ba2b2646aa78cabf7e3279f/com.ibm.jbatch.container/src/main/java/com/ibm/jbatch/container/services/impl/JDBCPersistenceManagerImpl.java#L392)
 * can use "SET SCHEMA ?" for most databases and "USE SCHEMA" for mysql
 * no support from mssql (afaik) and oracle should support it even though there is an exception for it in the above code
 * checked postgres, db2, oracle
 * the schema is valid for the entire connection (that means any statements that follow on the same connection)
 * set if explicitly with a dedicated service? only downside is that it does not work for autocommit connections (but who uses that anyway...)
 * alternative is to have it as input parameter for the jdbc service but that seems to imply that it is only valid for that statement
 * while in actuality it is for that statement and any that follows that does not explicitly override it.
 * 
 * TODO: If the result set is big, use windowed list and stream to file or stream straight from resultset if keeping the connection open is a viable option
 * 
 * There is an ugly dependency with the JDBCServiceManager: we set the sql first, because isInputGenerated and output are true by default
 * We scan the sql to generate the input.
 * Only afterwards do we set the actual type (if any) and disable the isInputGenerated
 * Because for automatic statements we first set the generated to false and only then set the sql, there were no input parameters
 * Currently updated the getInputParameters to generate them if they don't exist but it is not ideal...
 * 
 * Known issue: if you do getInput() and then setParameters(), the parameters in the original input are not updated!
 */
public class JDBCService implements DefinedService {
	
	private String connectionId, sql;
	
	private Structure input, output;
	private ComplexType parameters, results;
	private List<String> inputNames;
	private boolean isInputGenerated = true, isOutputGenerated = true;
	private Boolean validateInput, validateOutput;
	private DynamicDataSourceResolver dataSourceResolver;
	
	/**
	 * We currently do not support multiple columns but we could if we dynamically generate new documents containing the lists instead of just a list of long
	 */
	private String generatedColumn;
	
	public static final String CONNECTION = "connection";
	public static final String TRANSACTION = "transaction";
	public static final String PARAMETERS = "parameters";
	public static final String PROPERTIES = "properties";
	public static final String RESULTS = "results";
	public static final String OFFSET = "offset";
	public static final String LIMIT = "limit";
	public static final String INCLUDE_TOTAL_COUNT = "totalRowCount";
	public static final String TRACK_CHANGES = "trackChanges";
	public static final String LAZY = "lazy";
	public static final String GENERATED_KEYS = "generatedKeys";
	public static final String ROW_COUNT = "rowCount";
	public static final String TOTAL_ROW_COUNT = "totalRowCount";
	public static final String ORDER_BY = "orderBy";
	public static final String HAS_NEXT = "hasNext";
	public static final String AFFIX = "affix";
	
	private ChangeTracker changeTracker;
	
	private SimpleTypeWrapper wrapper = SimpleTypeWrapperFactory.getInstance().getWrapper();
	
	private String id;

	private Map<SQLDialect, Map<String, String>> preparedSql = new HashMap<SQLDialect, Map<String, String>>();
	private Map<SQLDialect, Map<String, String>> totalCountSql = new HashMap<SQLDialect, Map<String, String>>();
	
	public JDBCService(String id) {
		this.id = id;
	}
	
	public String getConnectionId() {
		return connectionId;
	}
	public void setConnectionId(String connectionId) {
		this.connectionId = connectionId;
	}
	public ModifiableComplexType getInput() {
		if (input == null) {
			synchronized(this) {
				if (input == null) {
					Structure input = new Structure();
					input.setName("input");
					input.add(new SimpleElementImpl<String>(CONNECTION, wrapper.wrap(String.class), input, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0)));
					input.add(new SimpleElementImpl<String>(TRANSACTION, wrapper.wrap(String.class), input, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0)));
					input.add(new SimpleElementImpl<Long>(OFFSET, wrapper.wrap(Long.class), input, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0)));
					input.add(new SimpleElementImpl<Integer>(LIMIT, wrapper.wrap(Integer.class), input, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0)));
					input.add(new SimpleElementImpl<String>(ORDER_BY, wrapper.wrap(String.class), input, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0), new ValueImpl<Integer>(MaxOccursProperty.getInstance(), 0)));
					input.add(new SimpleElementImpl<Boolean>(INCLUDE_TOTAL_COUNT, wrapper.wrap(Boolean.class), input, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0)));
					input.add(new SimpleElementImpl<Boolean>(HAS_NEXT, wrapper.wrap(Boolean.class), input, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0)));
					input.add(new SimpleElementImpl<Boolean>(TRACK_CHANGES, wrapper.wrap(Boolean.class), input, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0)));
					input.add(new SimpleElementImpl<Boolean>(LAZY, wrapper.wrap(Boolean.class), input, 
							new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0), 
							new ValueImpl<String>(CommentProperty.getInstance(), "When performing a select, the return value can be a lazy list based around a resultset.")));
					// temporarily(?) disabled because harder to do it in a table-specific way
					// perhaps supporting only pool-based prefixes is best?
//					input.add(new SimpleElementImpl<String>(AFFIX, wrapper.wrap(String.class), input, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0)));
					input.add(new ComplexElementImpl(PARAMETERS, getParameters(), input, new ValueImpl<Integer>(MaxOccursProperty.getInstance(), 0)));
					// allow a list of properties
					input.add(new ComplexElementImpl(PROPERTIES, (ComplexType) BeanResolver.getInstance().resolve(KeyValuePair.class), input, new ValueImpl<Integer>(MaxOccursProperty.getInstance(), 0), new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0)));
					this.input = input;
				}
			}
		}
		return input;
	}
	public ModifiableComplexType getOutput() {
		if (output == null) {
			synchronized(this) {
				if (output == null) {
					Structure output = new Structure();
					output.setName("output");
					output.add(new ComplexElementImpl(RESULTS, getResults(), output, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0), new ValueImpl<Integer>(MaxOccursProperty.getInstance(), 0)));
					output.add(new SimpleElementImpl<Long>(GENERATED_KEYS, wrapper.wrap(Long.class), output, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0), new ValueImpl<Integer>(MaxOccursProperty.getInstance(), 0)));
					output.add(new SimpleElementImpl<Long>(ROW_COUNT, wrapper.wrap(Long.class), output));
					output.add(new SimpleElementImpl<Long>(TOTAL_ROW_COUNT, wrapper.wrap(Long.class), output, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0)));
					output.add(new SimpleElementImpl<Boolean>(HAS_NEXT, wrapper.wrap(Boolean.class), output, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0)));
					this.output = output;
				}
			}
		}
		return output;
	}
	
	public ComplexType getParameters() {
		if (parameters == null) {
			DefinedStructure parameters = new DefinedStructure();
			parameters.setId(id + "." + PARAMETERS);
			parameters.setName("parameter");
			this.parameters = parameters;
		}
		return parameters;
	}
	
	public ComplexType getResults() {
		if (results == null) {
			DefinedStructure results = new DefinedStructure();
			results.setId(id + "." + RESULTS);
			results.setName("result");
			this.results = results;
		}
		return results;
	}
	
	public String getSql() {
		return sql;
	}
	
	public List<ValidationMessage> setSql(String sql) {
		List<ValidationMessage> messages = new ArrayList<ValidationMessage>();
		// if the sql is updated, we will regenerate the input
		if (this.sql == null || !this.sql.equals(sql)) {
			messages.addAll(regenerateInterfaceFromSQL(sql));
		}
		this.sql = sql;
		return messages;
	}

	String getPreparedSql(SQLDialect dialect, String sql) {
		if (!preparedSql.containsKey(dialect)) {
			synchronized(preparedSql) {
				if (!preparedSql.containsKey(dialect)) {
					preparedSql.put(dialect, new HashMap<String, String>());
				}
			}
		}
		if (!preparedSql.get(dialect).containsKey(sql)) {
			synchronized(preparedSql.get(dialect)) {
				if (!preparedSql.get(dialect).containsKey(sql)) {
					String value = dialect == null ? sql : dialect.rewrite(sql, getParameters(), getResults());
					// replace named parameters
					value = value.replaceAll("(?<!:):[\\w]+", "?");
					preparedSql.get(dialect).put(sql, value);
				}
			}
		}
		return preparedSql.get(dialect).get(sql);
	}
	
	String getTotalCountSql(SQLDialect dialect, String sql) {
		if (!totalCountSql.containsKey(dialect)) {
			synchronized(totalCountSql) {
				if (!totalCountSql.containsKey(dialect)) {
					totalCountSql.put(dialect, new HashMap<String, String>());
				}
			}
		}
		if (!totalCountSql.get(dialect).containsKey(sql)) {
			synchronized(totalCountSql.get(dialect)) {
				if (!totalCountSql.get(dialect).containsKey(sql)) {
					totalCountSql.get(dialect).put(sql, dialect == null ? SQLDialect.getDefaultTotalCountQuery(sql) : dialect.getTotalCountQuery(sql));
				}
			}
		}
		return totalCountSql.get(dialect).get(sql); 
	}
	
	/**
	 * Can't do select "*" atm!
	 * Perhaps have a boolean for it so the editor knows to allow more edits?
	 * But if you do select *, it is very hard to guarantee the order of the result fields anyway, how to map them?
	 * @return 
	 */
	private List<ValidationMessage> regenerateInterfaceFromSQL(String sql) {
		List<ValidationMessage> messages = new ArrayList<ValidationMessage>();
		
		if (isInputGenerated) {
			Structure parameters = (Structure) getParameters();
			
			// first remove all the current children but build a map of their information
			// the only information you can set is: type (choose which data type it is) & maxoccurs (it can be a list if you have "in (:var)"
			Map<String, Element<?>> inputElements = new HashMap<String, Element<?>>();
			Iterator<Element<?>> iterator = parameters.iterator();
			while (iterator.hasNext()) {
				Element<?> element = iterator.next();
				inputElements.put(element.getName(), element);
				iterator.remove();
			}
			preparedSql.clear();
			
			// regenerate
			if (sql != null) {
				// regenerate input
				Pattern pattern = Pattern.compile("(?<!:)[:$][\\w]+");
				Matcher matcher = pattern.matcher(sql);
				inputNames = new ArrayList<String>();
				while (matcher.find()) {
					String name = matcher.group().substring(1);
					// you can reuse the same parameter, the "inputNames" list can log it multiple times so it will be picked up and set as needed
					if (parameters.get(name) == null) {
						if (inputElements.containsKey(name)) {
							messages.addAll(parameters.add(inputElements.get(name)));
						}
						else {
							messages.addAll(parameters.add(new SimpleElementImpl<String>(name, wrapper.wrap(String.class), parameters)));
						}
					}
					if (matcher.group().startsWith(":")) {
						inputNames.add(name);
					}
				}
				
				// if there are no input parameters, set the min occurs to 0
				if (!parameters.iterator().hasNext()) {
					getInput().get(PARAMETERS).setProperty(new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0));	
				}
				else {
					getInput().get(PARAMETERS).setProperty(new ValueImpl<Integer>(MinOccursProperty.getInstance(), 1));
				}
			}
		}
		
		boolean isSelect = sql != null && sql.trim().toLowerCase().startsWith("select");
		boolean isWith = sql != null && sql.trim().toLowerCase().startsWith("with");
		
		// we have a CTE (common table expression)
		// we need to check if it is in function of a select or not
		if (isWith) {
			int depth = 0;
			// multiple brackets are combined into one part, e.g. ")))"
			for (String part : sql.trim().split("\\b")) {
				if (part.contains("(")) {
					depth += (part.length() - part.replace("(", "").length());
				}
				if (part.contains(")")) {
					depth -= (part.length() - part.replace(")", "").length());
				}
				// if we are at depth 0, we check for the keywords to see what the statement is
				if (depth == 0) {
					// yup, in a select
					if (part.matches("(?i).*\\bselect\\b.*")) {
						break;
					}
					else if (part.matches("(?i).*\\b(update|insert|delete)\\b.*")) {
						// this bit was added later, we don't want the code below for isWith (or isSelect) to trigger
						// in the initial version, CTE was only supported for selects
						isWith = false;
						break;
					}
				}
			}
		}
		
		if (isOutputGenerated) {
			Structure results = (Structure) getResults();
			Map<String, Element<?>> outputElements = new HashMap<String, Element<?>>();
			Iterator<Element<?>> iterator = results.iterator();
			while (iterator.hasNext()) {
				Element<?> element = iterator.next();
				outputElements.put(element.getName(), element);
				iterator.remove();
			}
			// regenerate output
			if (sql != null && (isSelect || isWith)) {
				int endIndex = -1;
				int startIndex = -1;
				if (isWith) {
					int depth = 0;
					int offset = 0;
					// multiple brackets are combined into one part, e.g. ")))"
					for (String part : sql.trim().split("\\b")) {
						if (part.contains("(")) {
							depth += (part.length() - part.replace("(", "").length());
						}
						if (part.contains(")")) {
							depth -= (part.length() - part.replace(")", "").length());
						}
						// we must get the index before the from
						if (endIndex < 0 && depth == 0 && part.matches("(?i).*\\bfrom\\b.*")) {
							endIndex = offset;
						}
						offset += part.length();
						// and after the select
						if (startIndex < 0 && depth == 0 && part.matches("(?i).*\\bselect\\b.*")) {
							startIndex = offset;
						}
					}
				}
				else {
					endIndex = sql.trim().toLowerCase().indexOf("from");
					startIndex = "select".length();
				}
				if (startIndex >= 0 && endIndex >= 0) {
					int depth = 0;
					String select = sql.trim().substring(startIndex, endIndex);
					int unnamed = 0;
					for (String part : select.split(",")) {
						int opening = part.length() - part.replace("(", "").length();
						depth += opening;
						int closing = part.length() - part.replace(")", "").length();
						depth -= closing;
						// if we are in a method call, ignore it
						if (depth > 0) {
							continue;
						}
						String [] subParts = part.split("\\bas\\b");
						String name = subParts.length > 1 ? subParts[1].trim() : subParts[0].trim();
						int indexOf = name.indexOf('.');
						if (indexOf >= 0) {
							name = name.substring(indexOf + 1);
						}
						if (name.trim().isEmpty()) {
							name = "unnamed" + unnamed++;
						}
						if (!name.matches("[\\w]+")) {
							name = name.replaceAll("[^\\w]+", "_");
						}
						name = camelCaseCharacter(name, '_');
						if (outputElements.containsKey(name)) {
							messages.addAll(results.add(outputElements.get(name)));
						}
						else {
							messages.addAll(results.add(new SimpleElementImpl<String>(name, wrapper.wrap(String.class), results)));
						}
					}
				}
			}
		}
		// if it's a select, you can only have one input
		// if it's not a select AND you are using replacement variables, you can also only set one
		// 		if you use the ":" notation, you reuse the native capabilities of prepared statement which can handle a batch
		// 		if you use the "$" notation, it is an actual string replace and is performed before the sql is sent to the prepared statement, as such it can not be done in batch!
		if (sql != null && (sql.matches(".*\\$[\\w]+.*") || isSelect || isWith)) {
			// set input parameters to single, can't do a batch of selects
			getInput().get(PARAMETERS).setProperty(new ValueImpl<Integer>(MaxOccursProperty.getInstance(), 1));
		}
		else {
			getInput().get(PARAMETERS).setProperty(new ValueImpl<Integer>(MaxOccursProperty.getInstance(), 0));
		}
		return messages;
	}
	
	List<String> scanForPreparedVariables(String sql) {
		Pattern pattern = Pattern.compile("(?<!:):[\\w]+");
		Matcher matcher = pattern.matcher(sql);
		List<String> inputNames = new ArrayList<String>();
		while (matcher.find()) {
			inputNames.add(matcher.group().substring(1));
		}
		return inputNames;
	}
	
	List<String> getInputNames() {
		if (inputNames == null) {
			List<String> inputNames = new ArrayList<String>();
			Pattern pattern = Pattern.compile("(?<!:):[\\w]+");
			Matcher matcher = pattern.matcher(sql);
			inputNames = new ArrayList<String>();
			while (matcher.find()) {
				inputNames.add(matcher.group().substring(1));
			}
			this.inputNames = inputNames;
		}
		return inputNames;
	}

	@Override
	public Set<String> getReferences() {
		return new HashSet<String>(Arrays.asList(new String[] { connectionId }));
	}
	
	@Override
	public ServiceInterface getServiceInterface() {
		return new ServiceInterface() {
			@Override
			public ComplexType getInputDefinition() {
				return getInput();
			}
			@Override
			public ComplexType getOutputDefinition() {
				return getOutput();
			}
			@Override
			public ServiceInterface getParent() {
				return null;
			}
		};
	}
	@Override
	public ServiceInstance newInstance() {
		return new JDBCServiceInstance(this);
	}
	
	@Override
	public String getId() {
		return id;
	}

	public void setParameters(ComplexType parameters) {
		this.parameters = parameters;
		if (input != null) {
			((ComplexElementImpl) input.get(PARAMETERS)).setType(getParameters());
		}
	}

	public void setResults(ComplexType results) {
		this.results = results;
		if (output != null) {
			((ComplexElementImpl) output.get(RESULTS)).setType(getResults());
		}
	}

	public boolean isInputGenerated() {
		return isInputGenerated;
	}

	public void setInputGenerated(boolean isInputGenerated) {
		this.isInputGenerated = isInputGenerated;
	}

	public boolean isOutputGenerated() {
		return isOutputGenerated;
	}

	public void setOutputGenerated(boolean isOutputGenerated) {
		this.isOutputGenerated = isOutputGenerated;
	}

	public Boolean getValidateInput() {
		return validateInput;
	}

	public void setValidateInput(Boolean validateInput) {
		this.validateInput = validateInput;
	}

	public Boolean getValidateOutput() {
		return validateOutput;
	}

	public void setValidateOutput(Boolean validateOutput) {
		this.validateOutput = validateOutput;
	}

	public String getGeneratedColumn() {
		return generatedColumn;
	}

	public void setGeneratedColumn(String generatedColumn) {
		this.generatedColumn = generatedColumn;
	}
	
	public static String camelCaseCharacter(String name, char character) {
		StringBuilder builder = new StringBuilder();
		int index = -1;
		int lastIndex = 0;
		boolean first = true;
		while ((index = name.indexOf(character, index + 1)) > lastIndex) {
			String substring = name.substring(lastIndex, index);
			if (substring.isEmpty()) {
				continue;
			}
			else if (first) {
				builder.append(substring);
				first = false;
			}
			else {
				builder.append(substring.substring(0, 1).toUpperCase() + substring.substring(1));
			}
			lastIndex = index + 1;
		}
		if (lastIndex < name.length() - 1) {
			if (first) {
				builder.append(name.substring(lastIndex));
			}
			else {
				builder.append(name.substring(lastIndex, lastIndex + 1).toUpperCase()).append(name.substring(lastIndex + 1));
			}
		}
		return builder.toString();
	}

	public ChangeTracker getChangeTracker() {
		return changeTracker;
	}

	public void setChangeTracker(ChangeTracker changeTracker) {
		this.changeTracker = changeTracker;
	}

	public DynamicDataSourceResolver getDataSourceResolver() {
		return dataSourceResolver;
	}

	public void setDataSourceResolver(DynamicDataSourceResolver dataSourceResolver) {
		this.dataSourceResolver = dataSourceResolver;
	}

	private String getNormalizedDepth(String sql, int wantedDepth) {
		StringBuilder builder = new StringBuilder();
		int depth = 0;
		for (int i = 0; i < sql.length(); i++) {
			if ('(' == sql.charAt(i)) {
				depth++;
			}
			else if ('(' == sql.charAt(i)) {
				depth--;
			}
			else if (depth == wantedDepth) {
				builder.append(sql.charAt(i));
			}
		}
		return builder.toString().replaceAll("[\\s]+", " ").trim().toLowerCase();
	}
	
	// expand a select * into actual fields if you have a defined output
	public String expandSql(String sql) {
		// only expand if we did not generate the output, we must have a defined value
		if (!this.isOutputGenerated()) {
			String base = this.getNormalizedDepth(sql, 0);
			// if we do a select *, we want to dynamically match the output definition
			if (base.startsWith("select * from ") || base.startsWith("select distinct * from")) {
				List<String> fromBlacklist = Arrays.asList(",", "left", "outer", "inner", "join", "right", "on");
				List<ComplexType> types = new ArrayList<ComplexType>();
				ComplexType result = getResults();
				while (result != null) {
					types.add(result);
					result = (ComplexType) result.getSuperType();
				}
				Collections.reverse(types);
				List<Element<?>> inherited = new ArrayList<Element<?>>();
				StringBuilder select = new StringBuilder();
				String from = base.replaceAll("^select (?:distinct |)\\* from (.*?)\\b(?:where|order by|group by|limit|offset|having|window|union|except|intersect|fetch|for update|for share|$)\\b.*", "$1");
				for (ComplexType type : types) {
					Boolean value = ValueUtils.getValue(HiddenProperty.getInstance(), type.getProperties());
					
					String typeName = JDBCServiceInstance.uncamelify(getName(type.getProperties()));
					String bindingName = typeName;
					
					// find the binding name if necessary
					if (value == null || !value) {
						String[] split = from.split("\\b" + typeName + "\\b(?!\\.)", -1);
						if (split.length > 2) {
							throw new IllegalStateException("Can not expand select *, there are multiple bindings for table: " + typeName);
						}
						else if (split.length < 2) {
							throw new IllegalStateException("Can not expand select *, no binding found for table: " + typeName);
						}
						String possibleName = split[1].trim();
						int firstSpace = possibleName.indexOf(' ');
						if (firstSpace >= 0) {
							possibleName = possibleName.substring(0, firstSpace);
							// must be a word
							if (!fromBlacklist.contains(possibleName) && possibleName.matches("[\\w]+")) {
								bindingName = possibleName;
							}
						}
						// if we have some content and no whitespace, it is probably the binding name
						else if (possibleName.length() > 0) {
							bindingName = possibleName;
						}
						// if we did not provide an alias, we want to check if we added an affix
						if (bindingName.equals(typeName)) {
							if (split[0].length() > 0 && split[0].charAt(split[0].length() - 1) == '~') {
								bindingName = "~" + bindingName;
							}
							else if (split[1].length() > 0 && split[1].charAt(0) == '~') {
								bindingName = bindingName + "~";
							}
						}
					}
					
					if (!inherited.isEmpty() && (value == null || !value)) {
						for (Element<?> child : inherited) {
							if (!select.toString().isEmpty()) {
								select.append(",\n");
							}
							select.append("\t" + bindingName + "." + JDBCServiceInstance.uncamelify(child.getName()));
						}
						inherited.clear();
					}
					for (Element<?> child : type) {
						if (value != null && value) {
							inherited.add(child);
						}
						else {
							if (!select.toString().isEmpty()) {
								select.append(",\n");
							}
							select.append("\t" + bindingName + "." + JDBCServiceInstance.uncamelify(child.getName()));
						}
					}
				}
				int depth = 0;
				for (int i = 0; i < sql.length(); i++) {
					if (sql.charAt(i) == '(') {
						depth++;
					}
					else if (sql.charAt(i) == ')') {
						depth--;
					}
					else if (sql.charAt(i) == '*' && depth == 0) {
						return sql.substring(0, i) + "\n" + select.toString() + "\n" + sql.substring(i + 1);
					}
				}
			}
		}
		return sql;
	}
	
	public static String getName(Value<?>...properties) {
		String value = ValueUtils.getValue(CollectionNameProperty.getInstance(), properties);
		if (value == null) {
			value = ValueUtils.getValue(NameProperty.getInstance(), properties);
		}
		return value;
	}
}
