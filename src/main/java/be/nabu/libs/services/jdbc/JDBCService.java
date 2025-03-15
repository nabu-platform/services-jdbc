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

import be.nabu.libs.artifacts.ExceptionDescriptionImpl;
import be.nabu.libs.artifacts.api.ArtifactWithExceptions;
import be.nabu.libs.artifacts.api.ExceptionDescription;
import be.nabu.libs.artifacts.api.ExceptionDescription.ExceptionType;
import be.nabu.libs.property.ValueUtils;
import be.nabu.libs.property.api.Value;
import be.nabu.libs.services.api.DefinedService;
import be.nabu.libs.services.api.ServiceInstance;
import be.nabu.libs.services.api.ServiceInterface;
import be.nabu.libs.services.jdbc.api.ChangeTracker;
import be.nabu.libs.services.jdbc.api.DynamicDataSourceResolver;
import be.nabu.libs.services.jdbc.api.SQLDialect;
import be.nabu.libs.services.jdbc.api.Statistic;
import be.nabu.libs.types.DefinedTypeResolverFactory;
import be.nabu.libs.types.SimpleTypeWrapperFactory;
import be.nabu.libs.types.TypeUtils;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.DefinedType;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.api.KeyValuePair;
import be.nabu.libs.types.api.ModifiableComplexType;
import be.nabu.libs.types.api.SimpleTypeWrapper;
import be.nabu.libs.types.api.Type;
import be.nabu.libs.types.base.ComplexElementImpl;
import be.nabu.libs.types.base.Scope;
import be.nabu.libs.types.base.SimpleElementImpl;
import be.nabu.libs.types.base.TypeBaseUtils;
import be.nabu.libs.types.base.ValueImpl;
import be.nabu.libs.types.java.BeanResolver;
import be.nabu.libs.types.java.BeanType;
import be.nabu.libs.types.properties.CalculationProperty;
import be.nabu.libs.types.properties.CollectionNameProperty;
import be.nabu.libs.types.properties.CommentProperty;
import be.nabu.libs.types.properties.EnricherProperty;
import be.nabu.libs.types.properties.ForeignKeyProperty;
import be.nabu.libs.types.properties.ForeignNameProperty;
import be.nabu.libs.types.properties.HiddenProperty;
import be.nabu.libs.types.properties.MaxOccursProperty;
import be.nabu.libs.types.properties.MinOccursProperty;
import be.nabu.libs.types.properties.NameProperty;
import be.nabu.libs.types.properties.ScopeProperty;
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
public class JDBCService implements DefinedService, ArtifactWithExceptions {
	

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
	
	public static final String CHANGE_TRACKER = "changeTracker";
	public static final String CONNECTION = "connection";
	public static final String TRANSACTION = "transaction";
	public static final String PARAMETERS = "parameters";
	public static final String PROPERTIES = "properties";
	public static final String RESULTS = "results";
	public static final String OFFSET = "offset";
	public static final String LIMIT = "limit";
	public static final String INCLUDE_TOTAL_COUNT = "totalRowCount";
	public static final String INCLUDE_INLINE_COUNT = "inlineRowCount";
	public static final String INCLUDE_ESTIMATE_COUNT = "estimateRowCount";
	public static final String TRACK_CHANGES = "trackChanges";
	public static final String LAZY = "lazy";
	public static final String GENERATED_KEYS = "generatedKeys";
	public static final String ROW_COUNT = "rowCount";
	public static final String TOTAL_ROW_COUNT = "totalRowCount";
	public static final String ORDER_BY = "orderBy";
	public static final String STATISTICS = "statistics";
	public static final String HAS_NEXT = "hasNext";
	public static final String AFFIX = "affix";
	
	private ChangeTracker changeTracker;
	
	private SimpleTypeWrapper wrapper = SimpleTypeWrapperFactory.getInstance().getWrapper();
	
	private String id;

	private Map<SQLDialect, Map<String, String>> preparedSql = new HashMap<SQLDialect, Map<String, String>>();
	private Map<SQLDialect, Map<String, String>> totalCountSql = new HashMap<SQLDialect, Map<String, String>>();
	private Map<SQLDialect, Map<String, String>> statisticsSql = new HashMap<SQLDialect, Map<String, String>>();
	
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
					input.add(new SimpleElementImpl<String>(CONNECTION, wrapper.wrap(String.class), input, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0),
							new ValueImpl<Scope>(ScopeProperty.getInstance(), Scope.PRIVATE)));
					input.add(new SimpleElementImpl<String>(TRANSACTION, wrapper.wrap(String.class), input, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0),
							new ValueImpl<Scope>(ScopeProperty.getInstance(), Scope.PRIVATE)));
					input.add(new SimpleElementImpl<Long>(OFFSET, wrapper.wrap(Long.class), input, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0)));
					input.add(new SimpleElementImpl<Integer>(LIMIT, wrapper.wrap(Integer.class), input, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0)));
					input.add(new SimpleElementImpl<String>(ORDER_BY, wrapper.wrap(String.class), input, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0), new ValueImpl<Integer>(MaxOccursProperty.getInstance(), 0)));
					input.add(new SimpleElementImpl<String>(STATISTICS, wrapper.wrap(String.class), input, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0), new ValueImpl<Integer>(MaxOccursProperty.getInstance(), 0)));
					input.add(new SimpleElementImpl<Boolean>(INCLUDE_TOTAL_COUNT, wrapper.wrap(Boolean.class), input, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0)));
					input.add(new SimpleElementImpl<Boolean>(INCLUDE_INLINE_COUNT, wrapper.wrap(Boolean.class), input, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0)));
					input.add(new SimpleElementImpl<Boolean>(INCLUDE_ESTIMATE_COUNT, wrapper.wrap(Boolean.class), input, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0)));
					input.add(new SimpleElementImpl<Boolean>(HAS_NEXT, wrapper.wrap(Boolean.class), input, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0),
							new ValueImpl<Scope>(ScopeProperty.getInstance(), Scope.PRIVATE)));
					input.add(new SimpleElementImpl<Boolean>(TRACK_CHANGES, wrapper.wrap(Boolean.class), input, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0),
							new ValueImpl<Scope>(ScopeProperty.getInstance(), Scope.PRIVATE)));
					input.add(new SimpleElementImpl<String>(CHANGE_TRACKER, wrapper.wrap(String.class), input, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0),
							new ValueImpl<Scope>(ScopeProperty.getInstance(), Scope.PRIVATE)));
					input.add(new SimpleElementImpl<Boolean>(LAZY, wrapper.wrap(Boolean.class), input, 
							new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0), 
							new ValueImpl<String>(CommentProperty.getInstance(), "When performing a select, the return value can be a lazy list based around a resultset."),
							new ValueImpl<Scope>(ScopeProperty.getInstance(), Scope.PRIVATE)));
					// temporarily(?) disabled because harder to do it in a table-specific way
					// perhaps supporting only pool-based prefixes is best?
//					input.add(new SimpleElementImpl<String>(AFFIX, wrapper.wrap(String.class), input, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0)));
					input.add(new ComplexElementImpl(AFFIX, (ComplexType) BeanResolver.getInstance().resolve(AffixInput.class), input, new ValueImpl<Integer>(MaxOccursProperty.getInstance(), 0),
							new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0)));
					input.add(new ComplexElementImpl(PARAMETERS, getParameters(), input, new ValueImpl<Integer>(MaxOccursProperty.getInstance(), 0),
							new ValueImpl<Integer>(MinOccursProperty.getInstance(), isParameterOptional() ? 0 : 1)));
					// allow a list of properties
					input.add(new ComplexElementImpl(PROPERTIES, (ComplexType) BeanResolver.getInstance().resolve(KeyValuePair.class), input, new ValueImpl<Integer>(MaxOccursProperty.getInstance(), 0), new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0),
							new ValueImpl<Scope>(ScopeProperty.getInstance(), Scope.PRIVATE)));
					this.input = input;
				}
			}
		}
		return input;
	}
	private boolean isParameterOptional() {
		boolean hasMandatory = false;
		for (Element<?> child : getParameters()) {
			if (ValueUtils.getValue(MinOccursProperty.getInstance(), child.getProperties()) >= 1) {
				hasMandatory = true;
				break;
			}
		}
		return !hasMandatory;
	}
	public ModifiableComplexType getOutput() {
		if (output == null) {
			synchronized(this) {
				if (output == null) {
					Structure output = new Structure();
					output.setName("output");
					output.add(new ComplexElementImpl(RESULTS, getResults(), output, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0), new ValueImpl<Integer>(MaxOccursProperty.getInstance(), 0)));
					output.add(new ComplexElementImpl(STATISTICS, (ComplexType) BeanResolver.getInstance().resolve(Statistic.class), output, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0), new ValueImpl<Integer>(MaxOccursProperty.getInstance(), 0)));
					output.add(new SimpleElementImpl<Long>(GENERATED_KEYS, wrapper.wrap(Long.class), output, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0), new ValueImpl<Integer>(MaxOccursProperty.getInstance(), 0)));
					output.add(new SimpleElementImpl<Long>(ROW_COUNT, wrapper.wrap(Long.class), output));
					output.add(new SimpleElementImpl<Long>(TOTAL_ROW_COUNT, wrapper.wrap(Long.class), output, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0)));
					output.add(new SimpleElementImpl<Boolean>(HAS_NEXT, wrapper.wrap(Boolean.class), output, new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0),
							new ValueImpl<Scope>(ScopeProperty.getInstance(), Scope.PRIVATE)));
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
		// UPDATE: always regenerate input because we want to correctly indicate whether or not the parameters are optional if you change optionality for the child elements
		// otherwise we might not expose it correctly over REST
//		if (this.sql == null || !this.sql.equals(sql)) {
			messages.addAll(regenerateInterfaceFromSQL(sql));
//		}
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
	
	String getStatisticsSql(SQLDialect dialect, String sql, List<String> fields) {
		if (!statisticsSql.containsKey(dialect)) {
			synchronized(statisticsSql) {
				if (!statisticsSql.containsKey(dialect)) {
					statisticsSql.put(dialect, new HashMap<String, String>());
				}
			}
		}
		// the query is specific to the fields given
		String key = sql + fields;
		if (!statisticsSql.get(dialect).containsKey(key)) {
			synchronized(statisticsSql.get(dialect)) {
				if (!statisticsSql.get(dialect).containsKey(key)) {
					statisticsSql.get(dialect).put(key, SQLDialect.getDefaultStatisticsQuery(sql, fields));
				}
			}
		}
		return statisticsSql.get(dialect).get(key); 
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
				Matcher matcher = pattern.matcher(cleanupStrings(sql));
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
			}
		}
		boolean isSelect = sql != null && sql.trim().toLowerCase().startsWith("select");
		boolean isUpdate = sql != null && sql.trim().toLowerCase().startsWith("update");
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
						isUpdate = part.equalsIgnoreCase("update");
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
				if (isWith || true) {
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
			// always update the minoccurs of the parameters
			getInput().get(PARAMETERS).setProperty(new ValueImpl<Integer>(MinOccursProperty.getInstance(), isParameterOptional() ? 0 : 1));
		}
		else {
			getInput().get(PARAMETERS).setProperty(new ValueImpl<Integer>(MaxOccursProperty.getInstance(), 0));
		}
		
		Element<?> element = getInput().get("language");
		if (isUpdate || isSelect) {
			if (element == null) {
				getInput().add(new SimpleElementImpl<String>("language", wrapper.wrap(String.class), getInput(), new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0)));
			}
		}
		else if (element != null) {
			getInput().remove(element);
		}
		
		return messages;
	}
	
	List<String> scanForPreparedVariables(String sql) {
		Pattern pattern = Pattern.compile("(?<!:):[\\w]+");
		Matcher matcher = pattern.matcher(cleanupStrings(sql));
		List<String> inputNames = new ArrayList<String>();
		while (matcher.find()) {
			inputNames.add(matcher.group().substring(1));
		}
		return inputNames;
	}

	// @2023-12-07: apparently nobody ever noticed that things like to_date(:myVar, 'HH:mm') would result in a variable called "mm"
	// anyway, we want to strip out the strings presumably, note that two single quotes next to one another is an escaped quote and does not count
	private String cleanupStrings(String sql) {
		return sql.replaceAll("'[^']*?'", "");
	}
	
	public List<String> getInputNames() {
		if (inputNames == null) {
			this.inputNames = scanForPreparedVariables(sql);
//			List<String> inputNames = new ArrayList<String>();
//			Pattern pattern = Pattern.compile("(?<!:):[\\w]+");
//			Matcher matcher = pattern.matcher(sql);
//			inputNames = new ArrayList<String>();
//			while (matcher.find()) {
//				inputNames.add(matcher.group().substring(1));
//			}
//			this.inputNames = inputNames;
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
			// if the parameters are not a list (so a select), we may need to update the "mandatoriness" of the input depending on the new definition
			if (ValueUtils.getValue(MaxOccursProperty.getInstance(), input.get(PARAMETERS).getProperties()) == 1) {
				getInput().get(PARAMETERS).setProperty(new ValueImpl<Integer>(MinOccursProperty.getInstance(), isParameterOptional() ? 0 : 1));
			}
		}
	}

	public void setResults(ComplexType results) {
		this.results = results;
		if (results instanceof DefinedStructure && ((DefinedStructure) results).getId() == null) {
			((DefinedStructure) results).setId(id + "." + RESULTS);
		}
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

	private static String getNormalizedDepth(String sql, int wantedDepth, boolean cleanup) {
		StringBuilder builder = new StringBuilder();
		int depth = 0;
		for (int i = 0; i < sql.length(); i++) {
			if ('(' == sql.charAt(i)) {
				depth++;
			}
			else if (')' == sql.charAt(i)) {
				depth--;
			}
			else if (depth == wantedDepth) {
				builder.append(sql.charAt(i));
			}
		}
		if (cleanup) {
			return builder.toString().replaceAll("[\\s]+", " ").trim().toLowerCase();
		}
		return builder.toString();
	}
	
	private String getBindingName(String fieldName, List<ComplexType> types, Map<ComplexType, String> bindingNames) {
		return getBindingName(fieldName, types, bindingNames, true);
	}
	
	private String getBindingName(String fieldName, List<ComplexType> types, Map<ComplexType, String> bindingNames, boolean throwException) {
		// this assumes from most super type to most extended type
		for (ComplexType type : types) {
			Boolean value = ValueUtils.getValue(HiddenProperty.getInstance(), type.getProperties());
			if (value != null && value) {
				continue;
			}
			Element<?> element = type.get(fieldName);
			if (element != null) {
				return bindingNames.get(type);
			}
		}
		if (throwException) {
			throw new IllegalArgumentException("The field '" + fieldName + "' does not belong to any of the types in the extension hierarchy");
		}
		return null;
	}
	
	// hard to make work correctly with CTE's atm, support can be added if necessary later on
	public static String injectTotalCount(String sql) {
		sql = sql.trim();
		if (sql.startsWith("select")) {
			// limit it to the "select...from", we may have stripped too much later on (with nested queries etc) to be relevant
			// note that we may also have stripped functions etc in the select! e.g. date_trunc()
			String select = sql.replaceAll("(?s)(?i)^(select\\b.*?[\\s]*\\bfrom)\\b.*", "$1");
			// if we have a distinct select, we need to wrap it, otherwise the inline count will count WITHOUT distinct applied
			// note however that wrapping it, requires wrapping the CORRECT part of the query which can not be deduced from the normalizeddepth return (this strips anything not at level 0, including nested queries etc)
			// in that case, the full sql MUST be wrappable (looking at your CTE...)
			if (select.matches("(?s)(?i)^select[\\s]+distinct\\b.*")) {
				sql = "select *, count(1) over () as injected_inline_total_count from (" + sql + ")";
			}
			else {
				int endIndex = -1;
				int offset = 0;
				int depth = 0;
				for (String part : sql.split("\\bfrom\\b")) {
					if (part.contains("(")) {
						depth += (part.length() - part.replace("(", "").length());
					}
					if (part.contains(")")) {
						depth -= (part.length() - part.replace(")", "").length());
					}
					offset += part.length();
					// we must get the index before the from
					if (endIndex < 0 && depth == 0) {
						endIndex = offset;
					}
					// if we go beyond this piece, add the from offset also
					offset += "from".length();
					// in this particular case, no need to continue, we have found our "from"
					if (endIndex >= 0) {
						break;
					}
				}
				if (endIndex >= 0) {
					sql = sql.substring(0, endIndex).trim() + ",\n\tcount(1) over () as injected_inline_total_count\n" + sql.substring(endIndex);
				}
				else {
					return null;
				}
//				String newBase = base.replaceAll("(?s)(?i)^(select\\b)(.*?)([\\s]*\\bfrom)\\b", "$1$2,\n\tcount(1) over () as injected_inline_total_count$3");
//				sql = sql.replace(base, newBase);
			}
			return sql;
		}
		return null;
	}
	
	// expand a select * into actual fields if you have a defined output
	public String expandSql(String sql, List<String> orderBys) {
		// currently we only expand into the table itself
		// this means if you have a foreign key to "user" which extends "node", you can currently only target fields in the user table, not yet in the node table
		// this is possible but requires a lot more coding to detect in which table it is, make sure binding names reflect that (you can now use a single foreign key to bind to multiple tables, the current naming scheme is not sufficient)
		// and make sure we resolve how the tables are bound
		boolean supportForeigNameExpansion = true;
		// only expand if we did not generate the output, we must have a defined value
		if (!this.isOutputGenerated()) {
			String base = getNormalizedDepth(sql, 0, true);
			// if we do a select *, we want to dynamically match the output definition
			if (base.startsWith("select * from ") || base.startsWith("select distinct * from")) {
				List<String> fromBlacklist = Arrays.asList(",", "left", "outer", "inner", "join", "right", "on");
				ComplexType result = getResults();
				List<ComplexType> types = new ArrayList<ComplexType>();
				while (result != null) {
					types.add(result);
					result = (ComplexType) result.getSuperType();
					// if we have reached the java.lang.Object class, we stop
					if (result instanceof BeanType && Object.class.equals(((BeanType<?>) result).getBeanClass())) {
						break;
					}
				}
				Collections.reverse(types);
				List<Element<?>> inherited = new ArrayList<Element<?>>();
				StringBuilder select = new StringBuilder();
				String from = base.replaceAll("^select (?:distinct |)\\* from (.*?)\\b(?:where|order by|group by|limit|offset|having|window|union|except|intersect|fetch|for update|for share|$)\\b.*", "$1");
				StringBuilder adhocBindings = new StringBuilder();
				
				Map<ComplexType, String> bindingNames = new HashMap<ComplexType, String>();
				
				// we keep track of which table each selected field is in as we bind it
				// if we then need to reference it for some other binding, it is easy to figure out
				// this was added to support more complex foreign name bindings where you add a new field that references a local field but adds a foreign key
				// TODO: currently you can only use fields that are also used in the resultset, if they are already filtered out, this won't work
				// while this is a quick fix and should work for now, in the future we should resolve the fields against the actual types without needing to select them
				Map<String, String> selectedFieldTables = new HashMap<String, String>();
				
				for (ComplexType type : types) {
					Boolean hiddenBoolean = ValueUtils.getValue(HiddenProperty.getInstance(), type.getProperties());
					
					String typeName = JDBCServiceInstance.uncamelify(JDBCUtils.getTypeName(type, true));
					String bindingName = typeName;
					
					// if we are not explicitly hidden, we are not the last type in the line and have no explicit collection name, we are skipped
					// @2024-03-11 not sure why we are still picking tables that are not explicitly marked, don't think there ever was a usecase for it
					// the problem is, sometimes we want core types that are extended into database types that are in and off themselves not database tables
					if (hiddenBoolean == null && types.indexOf(type) < types.size() - 1) {
						String collectionName = ValueUtils.getValue(CollectionNameProperty.getInstance(), type.getProperties());
						if (collectionName == null) {
							hiddenBoolean = true;
						}
					}
					
					// find the binding name if necessary
					if (hiddenBoolean == null || !hiddenBoolean) {
						String[] split = from.split("\\b" + typeName + "\\b(?!\\.)", -1);
						// if there are multiple, we are going to assume the first binding is the correct one!
						// too many times we have run into the issue that we want to do additional binding and can't
						// in the future we can make this toggleable or have some warning about it
//						if (split.length > 2) {
//							throw new IllegalStateException("Can not expand select *, there are multiple bindings for table: " + typeName);
//						}
						if (split.length < 2) {
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
					
					bindingNames.put(type, bindingName);
					
					if (!inherited.isEmpty() && (hiddenBoolean == null || !hiddenBoolean)) {
						for (Element<?> child : inherited) {
							if (!select.toString().isEmpty()) {
								select.append(",\n");
							}
							select.append("\t" + bindingName + "." + JDBCServiceInstance.uncamelify(child.getName()));
							selectedFieldTables.put(child.getName(), bindingName);
						}
						inherited.clear();
					}
					Map<String, List<ComplexType>> allJoinedTables = new HashMap<String, List<ComplexType>>();
					
					// if you want to import a field, assign a custom foreign key to it and reuse that imported field in a new binding
					// we can't use the child name, because it is not linked to any actual table, we need to use the bit we put before the "as" when selecting it
					// this hashmap keeps track of that bit so we can reuse it
					Map<String, String> importedChildren = new HashMap<String, String>();
					
					for (Element<?> child : type) {
						String calculation = ValueUtils.getValue(CalculationProperty.getInstance(), child.getProperties());
						if (calculation != null && calculation.trim().isEmpty()) {
							calculation = null;
						}
						// if we don't get the same child back from the top level parent, we probably restricted it at some level
						if (!child.equals(getResults().get(child.getName()))) {
							continue;
						}
						// if it is hidden, we probably inherit it
						if (hiddenBoolean != null && hiddenBoolean) {
							inherited.add(child);
						}
						// if we support foreign name expansion, we need to get creative...
						else if (supportForeigNameExpansion && child.getProperty(ForeignNameProperty.getInstance()) != null && child.getProperty(ForeignNameProperty.getInstance()).getValue() != null) {
							String foreignName = child.getProperty(ForeignNameProperty.getInstance()).getValue();
							List<String> foreignNameTables = JDBCUtils.getForeignNameTables(foreignName);
							// if we have no tables, it might be an empty string or...you might be referring to another field in the current type!
							// in theory we could use this to do computations like a fullName with foreign name: firstname || ' ' || lastName
							// we can't use this for calculations atm because we do an uncamelify on the foreign name, if we made that smarter, we could do some basic calculations with this as well...
							if (foreignNameTables == null) {
								if (foreignName != null && !foreignName.trim().isEmpty()) {
									if (!select.toString().isEmpty()) {
										select.append(",\n");
									}
									int lastIndexOf = foreignName.lastIndexOf('@');
									if (lastIndexOf >= 0) {
										foreignName = foreignName.substring(0, lastIndexOf);
									}
									// we reuse an existing one
									if (importedChildren.containsKey(foreignName)) {
										importedChildren.put(child.getName(), importedChildren.get(foreignName));
										select.append("\t" + importedChildren.get(foreignName) + " as " + JDBCServiceInstance.uncamelify(child.getName()));	
									}
									else {
										String bindingToUse = getBindingName(foreignName, types, bindingNames);
										String importedBinding = (calculation == null ? "" : calculation + "(") + bindingToUse + "." + JDBCServiceInstance.uncamelify(foreignName) + (calculation == null ? "" : ")");
										importedChildren.put(child.getName(), importedBinding);
										select.append("\t" + importedBinding + " as " + JDBCServiceInstance.uncamelify(child.getName()));
									}
								}
							}
							else {
								List<String> foreignNameFields = JDBCUtils.getForeignNameFields(foreignName);
								ComplexType typeToSearch = JDBCUtils.getForeignLookupType(foreignName);
								if (typeToSearch == null) {
									typeToSearch = type;
								}
								String lastBindingName = bindingName;
								boolean optional = false;
								for (int i = 0; i < foreignNameTables.size(); i++) {
									String foreignNameTable = foreignNameTables.get(i);
									// we always resolve the foreign keys just in case we have to bind tables further down
									String localField = foreignNameFields.get(i);
									// we can get it from the current type (it may be restricted etc in child types)
									Element<?> element = typeToSearch.get(localField);
									
									// if we are at the first of the bindings, we need to figure out which type we need
									// for instance of B extends A and C extends B and C adds a binding based on a field in A, if we don't check it, it will attempt to bind to the field in C
									// which doesn't exist of course
									if (i == 0) {
										lastBindingName = getBindingName(localField, types, bindingNames);
									}
									// you can also bind to any table in the previous one
									else {
										Map<ComplexType, String> theNames = new HashMap<ComplexType, String>();
										for (int j = 0; j < allJoinedTables.get(lastBindingName).size(); j++) {
											String name = lastBindingName;
											if (j > 0) {
												name += j;
											}
											theNames.put(allJoinedTables.get(lastBindingName).get(j), name);
										}
										List<ComplexType> list = new ArrayList<ComplexType>(allJoinedTables.get(lastBindingName));
										// it can only go wrong if we have multiple tables bound
										// the fact that this was not on, allowed me to find a complex problem earlier, so maybe leave it off for now?
//										if (list.size() > 1) {
											Collections.reverse(list);
											// we take the previous field, not the current one
											lastBindingName = getBindingName(localField, list, theNames);
//										}
									}
									
									if (element == null) {
										throw new IllegalArgumentException("The field '" + child.getName() + "' has a foreign name linked to the field '" + localField + "' which does not exist in this type: " + typeToSearch);
									}
									Value<String> elementAlias = element.getProperty(ForeignNameProperty.getInstance());

									// if we have a simple alias, we need to use that!
									// the reason for this is the dynamic foreign key problem (see JDBCUtils description)
									if (elementAlias != null && elementAlias.getValue() != null && !elementAlias.getValue().trim().isEmpty() && !elementAlias.getValue().contains(":")) {
										localField = elementAlias.getValue().trim();
									}
									
									// we expect a foreign key property on that field!
									Value<String> foreignKey = element.getProperty(ForeignKeyProperty.getInstance());
									if (foreignKey == null) {
										throw new IllegalArgumentException("The field '" + child.getName() + "' has a foreign name linked to the field '" + localField + "' which does not have a foreign key");
									}
									String[] split = foreignKey.getValue().split(":");
									if (split.length != 2) {
										throw new IllegalArgumentException("The field '" + child.getName() + "' has a foreign name linked to the field '" + localField + "' which does not have a valid foreign key");
									}
									DefinedType resolve = DefinedTypeResolverFactory.getInstance().getResolver().resolve(split[0]);
									if (resolve == null) {
										throw new IllegalArgumentException("The field '" + child.getName() + "' has a foreign name linked to the field '" + localField + "' but the foreign key type '" + split[0] + "' can not be resolved");
									}
									// always calculate the optional correctly, otherwise when we reuse optional bindings we might not detect them as such
									Value<Integer> minOccurs = element.getProperty(MinOccursProperty.getInstance());
									optional |= minOccurs != null && minOccurs.getValue() != null && minOccurs.getValue() <= 0;
									// if it does not yet contain the binding, we add it
									if (!adhocBindings.toString().matches("(?s).*\\b" + foreignNameTable + "\\b.*")) {
										// if our target is also an extension, let's join the parents as well
										// without digging deeper it is hard to say which tables we'll need
										if (!allJoinedTables.containsKey(foreignNameTable)) {
											allJoinedTables.put(foreignNameTable, JDBCUtils.getAllTypes((ComplexType) resolve));
										}
										for (int j = 0; j < allJoinedTables.get(foreignNameTable).size(); j++) {
											ComplexType typeToJoin = allJoinedTables.get(foreignNameTable).get(j);
											String targetTypeName = JDBCServiceInstance.uncamelify(JDBCUtils.getTypeName(typeToJoin, true));
											// if we are the first one, we are joined to the previous table based on the foreign name logic
											if (j == 0) {
												// if it references an imported field, we need that, it will include both the table and field name, which is why it doesn't fit neatly as a ternary in the other case
												if (importedChildren.containsKey(localField)) {
													adhocBindings.append("\n\t").append((!optional ? " join " : " left outer join ") + targetTypeName + " " + foreignNameTable + " on " + foreignNameTable + "." + JDBCServiceInstance.uncamelify(split[1]) + " = " + importedChildren.get(localField));
												}
												else {
													// if you join to an already selected field _and_ you are a first level join (so not a join to include extensions)
													// you may be referencing an "aliased" field which is a custom imported foreign key set on a an alias field (e.g. locationId to be more specific than parentId with a foreign key to locations table)
													// once you start binding extension tables, they never have any knowledge of the aliased fields
													// and additionally naming conflicts are likely to happen, especially because you usually select the id field (adding it to the selectedFields) but it is often also the joining-point
													// in the future we may only want to look at actually aliased fields (so basically foreign names without a foreign table -> cfr)
													// not sure why we are looking at all selected fields.
													String fieldBinding = foreignNameTable.startsWith("f0_") ? selectedFieldTables.get(localField) : null;
													
													// this does not work for remotely included fields? in fact this may cause problems when doing complex joins where names overlap with local names?
													// this does not work as expected, need to dig deeper
	//												if (fieldBinding == null) {
	//													fieldBinding = getBindingName(localField, types, bindingNames, false);
	//												}
													// the original fallback before fancy custom foreign keys were added
													if (fieldBinding == null) {
														fieldBinding = lastBindingName;
													}
													
													// the fallback to lastbinding name is how it used to work before (before fancy foreign key shizzle)
													// just make sure it keeps working
													// in case of weird binding choice, disable this to figure it out!
													adhocBindings.append("\n\t").append((!optional ? " join " : " left outer join ") + targetTypeName + " " + foreignNameTable + " on " + foreignNameTable + "." + JDBCServiceInstance.uncamelify(split[1]) + " = " + fieldBinding + "." + JDBCServiceInstance.uncamelify(localField));
												}
											}
											// if we are the next one, we are joined to the previous one based on some binding value
											else {
												List<String> binding = JDBCUtils.getBinding(typeToJoin, allJoinedTables.get(foreignNameTable).get(j - 1));
												String previousTable = foreignNameTable + (j == 1 ? "" : j - 1);
												adhocBindings.append("\n\t").append((!optional ? " join " : " left outer join ") + targetTypeName + " " + foreignNameTable + j + " on " + foreignNameTable + j + "." + JDBCServiceInstance.uncamelify(binding.get(0)) + " = " + previousTable + "." + JDBCServiceInstance.uncamelify(binding.get(1)));
												// in the original query we might be referencing a field from our expanded table, for instance suppose you are referencing f0_device_id.parent_id
												// but upon this expansion, it turns out that the parent_id is actually in one of the supertypes, e.g. device extends node, so parent_id is actually in f0_device_id1.parent_id
												// either the calling logic must know that we numerically increment the bindings when binding supertypes
												// or we auto-update any references to local fields in a supertype
												// we assume at this point that the chance of accidental conflict is really small
												for (Element<?> localChild : TypeUtils.getLocalChildren(typeToJoin)) {
													sql = sql.replaceAll("\\b" + Pattern.quote(foreignNameTable + "." + JDBCServiceInstance.uncamelify(localChild.getName())) + "\\b", foreignNameTable + j + "." + JDBCServiceInstance.uncamelify(localChild.getName()));
												}
											}
										}
									}
									// we need to bind against this table!
									lastBindingName = foreignNameTable;
									typeToSearch = (ComplexType) resolve;
								}
								// now we just bind the value
								if (!select.toString().isEmpty()) {
									select.append(",\n");
								}
								String foreignTableToBind = foreignNameTables.get(foreignNameTables.size() - 1);
								String foreignFieldToBind = foreignNameFields.get(foreignNameFields.size() - 1);
								List<ComplexType> list = new ArrayList<ComplexType>(allJoinedTables.get(foreignTableToBind));
								boolean isInRootTable = true;
								for (int j = 1; j < list.size(); j++) {
									// once we no longer find it, take the previous table, it was added there (hopefully!)
									if (list.get(j).get(foreignFieldToBind) == null) {
										isInRootTable = false;
										if (j == 1) {
											// do nothing, the table is the correct name
											break;
										}
										// otherwise we bind it to the previous table, which contained the field last
										else {
											foreignTableToBind += (j - 1);
											break;
										}
									}
								}
								// so if we looped over all the tables and the field is available in all, that means it is available in the root table
								// find it there!
								if (isInRootTable && list.size() > 1) {
									foreignTableToBind += list.size() - 1;
								}
								String importedBinding = (calculation == null ? "" : calculation + "(") + foreignTableToBind + "." + JDBCServiceInstance.uncamelify(foreignFieldToBind) + (calculation == null ? "" : ")");
								importedChildren.put(child.getName(), importedBinding);
								select.append("\t" + importedBinding + " as " + JDBCServiceInstance.uncamelify(child.getName()));
							}
						}
						else if (TypeBaseUtils.isEnriched(child)) {
							// we do nothing, it is enriched later on
						}
						else {
							if (!select.toString().isEmpty()) {
								select.append(",\n");
							}
							select.append("\t" + (calculation == null ? "" : calculation + "(") + bindingName + "." + JDBCServiceInstance.uncamelify(child.getName())
								+ (calculation == null ? "" : ") as " + JDBCServiceInstance.uncamelify(child.getName())));
							
							selectedFieldTables.put(child.getName(), bindingName);
						}
					}
				}
				boolean injectAdhoc = !adhocBindings.toString().isEmpty();
				int asteriskPosition = -1;
				int depth = 0;
				// we create a special version of the sql where we can do a nice regex split without coinciding with anything...
				StringBuilder special = injectAdhoc ? new StringBuilder() : null;
				for (int i = 0; i < sql.length(); i++) {
					if (sql.charAt(i) == '(') {
						depth++;
					}
					else if (sql.charAt(i) == ')') {
						depth--;
					}
					else if (sql.charAt(i) == '*' && depth == 0) {
						asteriskPosition = i;
					}
					if (injectAdhoc) {
						if (depth > 0 || (depth == 0 && sql.charAt(i) == ')')) {
							special.append("_");
						}
						else {
							special.append(sql.charAt(i));
						}
					}
				}
				if (injectAdhoc) {
					String firstPart = special.toString().split("\\b(?:where|order by|group by|limit|offset|having|window|union|except|intersect|fetch|for update|for share|$)")[0];
					return sql.substring(0, asteriskPosition) + "\n" + select.toString() + "\n" + sql.substring(asteriskPosition + 1, firstPart.length())
						+ adhocBindings.toString() + " " + sql.substring(firstPart.length());
				}
				else {
					return sql.substring(0, asteriskPosition) + "\n" + select.toString() + "\n" + sql.substring(asteriskPosition + 1);
				}
			}
		}
		return sql;
	}

	// we can get it from the current type (it may be restricted etc in child types)
	private Element<?> getFieldFrom(ComplexType type, String field) {
		Element<?> element = type.get(field);
		if (element == null && type.getSuperType() != null) {
			ComplexType superType = (ComplexType) type.getSuperType();
			while (superType != null) {
				element = superType.get(field);
				if (superType.getSuperType() instanceof ComplexType) {
					superType = (ComplexType) superType.getSuperType();
				}
				else {
					break;
				}
			}
		}
		return element;
	}
	
	public static String getName(Value<?>...properties) {
		String value = ValueUtils.getValue(CollectionNameProperty.getInstance(), properties);
		if (value == null) {
			value = ValueUtils.getValue(NameProperty.getInstance(), properties);
		}
		return value;
	}

	@Override
	public List<ExceptionDescription> getExceptions() {
		List<ExceptionDescription> descriptions = new ArrayList<ExceptionDescription>();
		descriptions.add(new ExceptionDescriptionImpl("JDBC-0", "JDBC-0", "No JDBC pool found", "Could not determine which JDBC connection to use", ExceptionType.DESIGN));
		descriptions.add(new ExceptionDescriptionImpl("JDBC-1", "JDBC-1", "Could not resolve JDBC pool", "Could not resolve the JDBC connection that should be used", ExceptionType.DESIGN));
		descriptions.add(new ExceptionDescriptionImpl("JDBC-2", "JDBC-2", "Can not find field metadata", "Could not find the metadata for the given field in either the structured input nor the key/value pairs", ExceptionType.DESIGN));
		descriptions.add(new ExceptionDescriptionImpl("JDBC-3", "JDBC-3", "Not a simple field", "The field is not a simple type", ExceptionType.DESIGN));
		descriptions.add(new ExceptionDescriptionImpl("JDBC-4", "JDBC-4", "Unknown date granularity", null, ExceptionType.DESIGN));
		descriptions.add(new ExceptionDescriptionImpl("JDBC-5", "JDBC-5", "Service input invalid", null));
		descriptions.add(new ExceptionDescriptionImpl("JDBC-6", "JDBC-6", "Service output invalid", null));
		descriptions.add(new ExceptionDescriptionImpl("JDBC-7", "JDBC-7", "Could not find sql", "Could not find the sql to run", ExceptionType.DESIGN));
		descriptions.add(new ExceptionDescriptionImpl("JDBC-8", "JDBC-8", "No primary key in definition", "Could not find primary key in the definition, it is needed for automatic change tracking", ExceptionType.DESIGN));
		descriptions.add(new ExceptionDescriptionImpl("JDBC-9", "JDBC-9", "No primary key in statement", "Could not find primary key in the statement, it is needed for automatic change tracking", ExceptionType.DESIGN));
		descriptions.add(new ExceptionDescriptionImpl("JDBC-10", "JDBC-10", "Could not determine table name", "Could not find the table name in the statement needed for automatic change tracking", ExceptionType.DESIGN));
		descriptions.add(new ExceptionDescriptionImpl("JDBC-11", "JDBC-11", "Primary key empty", "The primary key field was empty, a runtime value is necessary for automatic translations"));
		descriptions.add(new ExceptionDescriptionImpl("JDBC-12", "JDBC-12", "Could not find composite or aggregate", "Can only perform insert limitation if we have a composite or aggregate aggregation relation", ExceptionType.DESIGN));
		descriptions.add(new ExceptionDescriptionImpl("JDBC-13", "JDBC-13", "Can not find aggregation key", "Can not find the aggregation key value in the statement", ExceptionType.DESIGN));
		descriptions.add(new ExceptionDescriptionImpl("JDBC-14", "JDBC-14", "Could not determine table name", "Could not determine the table name in the statement needed for automatic translations", ExceptionType.DESIGN));
		descriptions.add(new ExceptionDescriptionImpl("JDBC-15", "JDBC-15", "Could not calculate amount of entries", "Could not calculate the amount of entries for the requested limit on aggregation", ExceptionType.DESIGN));
		descriptions.add(new ExceptionDescriptionImpl("JDBC-16", "JDBC-16", "Too many entries in input", "Too many entries in the input in accordance to the requested limit on aggregation", ExceptionType.BUSINESS));
		descriptions.add(new ExceptionDescriptionImpl("JDBC-17", "JDBC-17", "Invalid orderBy field", "Could not find the field requested in the orderBy"));
		descriptions.add(new ExceptionDescriptionImpl("JDBC-18", "JDBC-18", "Could not find primary key", "Could not find a primary key in the data definition, it is needed to perform automatic translations", ExceptionType.DESIGN));
		descriptions.add(new ExceptionDescriptionImpl("JDBC-19", "JDBC-19", "Can not combine automatic translation with lazy", "Can not combine lazy loading with automatic translations", ExceptionType.DESIGN));
		descriptions.add(new ExceptionDescriptionImpl("JDBC-20", "JDBC-20", "Could not find original", "Could not find the original data needed for automatic translations"));
		descriptions.add(new ExceptionDescriptionImpl("JDBC-21", "JDBC-21", "Can not find aggregation table name", "Can not find the table name needed for automatic aggregation limitation", ExceptionType.DESIGN));
		descriptions.add(new ExceptionDescriptionImpl("JDBC-22", "JDBC-22", "Too many entries in total", "Too many entries in accordance to the requested limit on aggregation", ExceptionType.BUSINESS));
		descriptions.add(new ExceptionDescriptionImpl("JDBC-23", "JDBC-23", "No primary key", "Could not find primary key in the input, it is needed for automatic change tracking"));
		descriptions.add(new ExceptionDescriptionImpl("JDBC-24", "JDBC-24", "Invalid result type", "Can not convert the result of the query to the given output type", ExceptionType.DESIGN));
		return descriptions;
	}
}
