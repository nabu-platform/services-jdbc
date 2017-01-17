package be.nabu.libs.services.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.libs.converter.ConverterFactory;
import be.nabu.libs.converter.api.Converter;
import be.nabu.libs.metrics.api.MetricInstance;
import be.nabu.libs.metrics.api.MetricTimer;
import be.nabu.libs.property.ValueUtils;
import be.nabu.libs.property.api.Value;
import be.nabu.libs.services.ServiceRuntime;
import be.nabu.libs.services.TransactionCloseable;
import be.nabu.libs.services.api.ExecutionContext;
import be.nabu.libs.services.api.ServiceException;
import be.nabu.libs.services.api.ServiceInstance;
import be.nabu.libs.services.api.Transactionable;
import be.nabu.libs.services.jdbc.api.ChangeSet;
import be.nabu.libs.services.jdbc.api.ChangeType;
import be.nabu.libs.services.jdbc.api.DataSourceWithDialectProviderArtifact;
import be.nabu.libs.services.jdbc.api.SQLDialect;
import be.nabu.libs.types.CollectionHandlerFactory;
import be.nabu.libs.types.ComplexContentWrapperFactory;
import be.nabu.libs.types.SimpleTypeWrapperFactory;
import be.nabu.libs.types.TypeUtils;
import be.nabu.libs.types.api.CollectionHandlerProvider;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.api.SimpleType;
import be.nabu.libs.types.base.SimpleElementImpl;
import be.nabu.libs.types.properties.AggregateProperty;
import be.nabu.libs.types.properties.CollectionNameProperty;
import be.nabu.libs.types.properties.PrimaryKeyProperty;
import be.nabu.libs.types.resultset.ResultSetCollectionHandler;
import be.nabu.libs.types.resultset.ResultSetWithType;
import be.nabu.libs.validator.api.Validation;
import be.nabu.libs.validator.api.ValidationMessage.Severity;
import be.nabu.libs.validator.api.Validator;

/**
 * TODO: add support for save points?
 * No support for parameters that are not complex content? hmm, should probably be a bean type at that point?
 * 		> but is it a bean or a beaninstance at runtime?
 */
public class JDBCServiceInstance implements ServiceInstance {

	public static final String METRIC_EXECUTION_TIME = "sqlExecutionTime";
	public static final String METRIC_MAP_TIME = "resultsMapTime";
	public static final Integer PREPARED_STATEMENT_ARRAY_SIZE = Integer.parseInt(System.getProperty("be.nabu.libs.services.jdbc.preparedStatementArraySize", "10"));
	
	private JDBCService definition;
	private Converter converter = ConverterFactory.getInstance().getConverter();
	private Logger logger = LoggerFactory.getLogger(getClass());

	JDBCServiceInstance(JDBCService definition) {
		this.definition = definition;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public ComplexContent execute(ExecutionContext executionContext, ComplexContent content) throws ServiceException {
		if (definition.getValidateInput() != null && definition.getValidateInput()) {
			Validator validator = content.getType().createValidator();
			List<? extends Validation<?>> validations = validator.validate(content);
			for (Validation<?> validation : validations) {
				if (validation.getSeverity() == Severity.CRITICAL || validation.getSeverity() == Severity.ERROR) {
					throw new ServiceException("JDBC-5", "The input is not valid: " + validations);
				}
			}
		}
		MetricInstance metrics = executionContext.getMetricInstance(definition.getId());
		
		boolean trackChanges = content == null || content.get(JDBCService.TRACK_CHANGES) == null || (Boolean) content.get(JDBCService.TRACK_CHANGES);
		boolean lazy = content != null && content.get(JDBCService.LAZY) != null && (Boolean) content.get(JDBCService.LAZY);
		
		JDBCDebugInformation debug = executionContext.isDebug() ? new JDBCDebugInformation() : null;
		// get the connection id, you can override this at runtime
		String connectionId = content == null ? null : (String) content.get(JDBCService.CONNECTION);
		if (connectionId == null) {
			connectionId = getDefinition().getConnectionId();
		}
		// do a lookup for providers that are within the same root folder (= application) as the root service
		if (connectionId == null && definition.getDataSourceResolver() != null) {
			connectionId = definition.getDataSourceResolver().getDataSourceId(definition.getId());
		}
		if (connectionId == null) {
			throw new ServiceException("JDBC-0", "No JDBC pool configured");
		}
		String transactionId = content == null ? null : (String) content.get(JDBCService.TRANSACTION);
		if (debug != null) {
			debug.setConnectionId(connectionId);
			debug.setTransactionId(transactionId);
		}
		// get the pool, we need to know if it's transactional
		DataSourceWithDialectProviderArtifact dataSourceProvider = executionContext.getServiceContext().getResolver(DataSourceWithDialectProviderArtifact.class).resolve(connectionId);
		if (dataSourceProvider == null) {
			throw new ServiceException("JDBC-1", "Can not find datasource provider: " + connectionId, connectionId);
		}
		// if it's not autocommitted, we need to check if there is already a transaction open on this resource for the given transaction id
		Connection connection = null;
		try {
			if (!dataSourceProvider.isAutoCommit()) {
				// if there is no open transaction, create one
				Transactionable transactionable = executionContext.getTransactionContext().get(transactionId, connectionId);
				if (transactionable == null) {
					connection = dataSourceProvider.getDataSource().getConnection();
					executionContext.getTransactionContext().add(transactionId, new ConnectionTransactionable(connectionId, connection));
				}
				else {
					connection = ((ConnectionTransactionable) transactionable).getConnection();
				}
			}
			// it's autocommitted, just start a new connection
			else {
				connection = dataSourceProvider.getDataSource().getConnection();
			}
			
			ComplexType type = (ComplexType) getDefinition().getInput().get(JDBCService.PARAMETERS).getType();
			boolean isBatch = type.isList(getDefinition().getInput().get(JDBCService.PARAMETERS).getProperties());
			Object object = content == null ? null : content.get(JDBCService.PARAMETERS);
			Collection<ComplexContent> parameters = null;
			if (object != null) {
				parameters = toContentCollection(object); 
			}
			String preparedSql = getDefinition().getSql();
			
			if (preparedSql == null) {
				throw new ServiceException("JDBC-7", "No sql found for: " + definition.getId() + ", expecting rewritten: " + definition.getSql());
			}

			// map the additional properties to a map
			Map<String, String> additional = new HashMap<String, String>();
			List keyValuePairs = content == null ? null : (List) content.get(JDBCService.PROPERTIES);
			if (keyValuePairs != null) {
				for (Object keyValuePair : keyValuePairs) {
					ComplexContent keyValuePairContent = keyValuePair instanceof ComplexContent ? (ComplexContent) keyValuePair : ComplexContentWrapperFactory.getInstance().getWrapper().wrap(keyValuePair);
					additional.put((String) keyValuePairContent.get("key"), (String) keyValuePairContent.get("value"));
				}
			}
			
			Pattern pattern = Pattern.compile("\\$[\\w]+");
			Matcher matcher = pattern.matcher(preparedSql);
			ComplexContent firstParameter = parameters != null && parameters.size() > 0 ? parameters.iterator().next() : null;
			boolean hasNewVariables = false;
			while (matcher.find()) {
				String name = matcher.group().substring(1);
				Object value = firstParameter == null ? null : firstParameter.get(name);
				if (value == null) {
					value = additional.get(name);
				}
				String replacement = converter.convert(value, String.class);
				// possible but unlikely to create false positives
				// worst case scenario we do runtime calculation of parameters instead of cached because of this
				hasNewVariables |= replacement.contains(":");
				preparedSql = preparedSql.replaceAll(Pattern.quote(matcher.group()), value == null ? "null" : replacement);
			}
			List<String> inputNames = hasNewVariables ? getDefinition().scanForPreparedVariables(preparedSql) : getDefinition().getInputNames();
			
			SQLDialect dialect = dataSourceProvider.getDialect();
			if (dialect == null) {
				dialect = new DefaultDialect();
			}
			
			// if the dialect does not support arrays, rewrite the statement if necessary
			for (String inputName : inputNames) {
				Element<?> element = type.get(inputName);
				if (element != null && !dialect.hasArraySupport(element) && element.getType().isList(element.getProperties())) {
					// we need to find the largest collection in the input
					int maxSize = 0;
					if (parameters != null) {
						for (ComplexContent parameter : parameters) {
							Object value = parameter.get(element.getName());
							if (value != null) {
								CollectionHandlerProvider handler = CollectionHandlerFactory.getInstance().getHandler().getHandler(value.getClass());
								if (handler == null) {
									throw new IllegalArgumentException("Could not find handler for '" + element.getName() + "' of type: " + value.getClass());
								}
								Collection asCollection = handler.getAsCollection(value);
								if (asCollection.size() > maxSize) {
									maxSize = asCollection.size();
								}
							}
						}
					}
					if (maxSize > 0) {
						StringBuilder builder = new StringBuilder();
						for (int i = 0; i < maxSize; i++) {
//								newNames.add(inputName + "[" + i + "]");
							if (i > 0) {
								builder.append(", ");
							}
							// don't append the index because the rewritten statements are cached on a string basis, this would unnecessarily enlarge that cache
							builder.append(":").append(inputName);
						}
						// repeat the last element a few times to get fewer "different" prepared statements
						// prepared statements are partly nice because they are cached, generating too many different ones however will oust the old ones from the cache
						for (int i = maxSize; i < PREPARED_STATEMENT_ARRAY_SIZE - (maxSize % PREPARED_STATEMENT_ARRAY_SIZE); i++) {
							builder.append(", :").append(inputName);
						}
						preparedSql = preparedSql.replaceAll(":" + inputName + "\\b", builder.toString());
					}
				}
			}
			
			preparedSql = getDefinition().getPreparedSql(dataSourceProvider.getDialect(), preparedSql);
			Integer offset = content == null ? null : (Integer) content.get(JDBCService.OFFSET);
			Integer limit = content == null ? null : (Integer) content.get(JDBCService.LIMIT);
			boolean nativeLimit = false;
			// the limit is for selects
			if (!isBatch && (offset != null || limit != null)) {
				if (dataSourceProvider.getDialect() != null) {
					String limitedSql = dataSourceProvider.getDialect().limit(preparedSql, offset, limit);
					if (limitedSql != null) {
						preparedSql = limitedSql;
						nativeLimit = true;
					}
				}
			}
			
			if (debug != null) {
				debug.setSql(preparedSql);
			}
			String generatedColumn = definition.getGeneratedColumn();
			PreparedStatement statement;
			if (generatedColumn != null) {
				statement = connection.prepareStatement(preparedSql, new String[] { generatedColumn });
			}
			else if (lazy) {
				try {
					statement = connection.prepareStatement(preparedSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				}
				catch (SQLFeatureNotSupportedException e) {
					logger.warn("The jdbc driver does not support scroll insensitive result sets, using forward only");
					statement = connection.prepareStatement(preparedSql);
				}
			}
			else {
				statement = connection.prepareStatement(preparedSql);
			}
			
			boolean batchInputAdded = false;
			try {
				if (parameters != null) {
					if (debug != null) {
						debug.setInputAmount(parameters.size());
					}
					for (ComplexContent parameter : parameters) {
						int index = 1;
						for (String inputName : inputNames) {
							Element<?> element = parameter.getType().get(inputName);
							Object value;
							if (element == null) {
								if (additional.containsKey(inputName)) {
									value = additional.get(inputName);
									// because we lack metadata, we assume it is always a string
									// we could try to magically parse it but that could lead to irritating edge cases
									// we could try to allow you to send objects instead of strings but then we might still need additional metadata about the objects (e.g. date)
									// so we strictly limit it to strings, in that edge case that you need actual types, you need to convert them in sql
									element = new SimpleElementImpl<String>(inputName, SimpleTypeWrapperFactory.getInstance().getWrapper().wrap(String.class), null);
								}
								else {
									throw new ServiceException("JDBC-2", "Can not determine the metadata for the field: " + inputName, inputName);
								}
							}
							else if (!(element.getType() instanceof SimpleType)) {
								throw new ServiceException("JDBC-3", "The field has a non-simple type: " + inputName, inputName);
							}
							else {
								value = parameter.get(inputName);
							}
							if (!dialect.hasArraySupport(element) && value != null && element.getType().isList(element.getProperties())) {
								CollectionHandlerProvider handler = CollectionHandlerFactory.getInstance().getHandler().getHandler(value.getClass());
								int amount = 0;
								Object last = null;
								for (Object single : handler.getAsIterable(value)) {
									dialect.setObject(statement, element, index++, single);
									amount++;
									last = single;
								}
								for (int i = amount; i < PREPARED_STATEMENT_ARRAY_SIZE - (amount % PREPARED_STATEMENT_ARRAY_SIZE); i++) {
									dialect.setObject(statement, element, index++, last);
								}
							}
							else {
								dialect.setObject(statement, element, index++, value);
							}
						}
						if (isBatch) {
							statement.addBatch();
							batchInputAdded = true;
						}
					}
				}
				// if there are input names but you did not provide anything, add nulls for everything
				else if (!inputNames.isEmpty()) {
					int index = 1;
					for (String inputName : inputNames) {
						Element<?> element = type.get(inputName);
						Integer sqlType = dialect.getSQLType(element);
						// could not perform a mapping, just pass it to the driver and hope it can figure it out
						if (sqlType == null) {
							logger.warn("Could not map instance class to native SQL type: {}", element.getName());
							statement.setObject(index++, null);	
						}
						else {
							statement.setNull(index++, sqlType);
						}
					}
					if (isBatch) {
						statement.addBatch();
						batchInputAdded = true;
					}
				}
	
				ComplexContent output = getDefinition().getOutput().newInstance();
				if (isBatch) {
					Element<?> primaryKey = null;
					List<Object> primaryKeys = null;
					StringBuilder selectBuilder = null;
					Map<Object, Map<String, Object>> original = null;
					String primaryKeyName = null;
					List<Object> missing = null;
					String tableName = null;
					
					// if limit is passed in for an insert, it acts as an amount delimiter in the database
					if (limit != null && preparedSql.trim().toLowerCase().startsWith("insert")) {
						// the key that has a collection aggregate set on it (owns relationship)
						Element<?> aggregateKey = null;
						for (Element<?> element : TypeUtils.getAllChildren(definition.getParameters())) {
							Value<String> aggregate = element.getProperty(AggregateProperty.getInstance());
							if (aggregate != null && aggregate.equals("composite")) {
								aggregateKey = element;
								break;
							}
						}
						if (aggregateKey == null) {
							throw new ServiceException("JDBC-12", "Can only limit inserts if a composite aggregate is found");
						}
						if (tableName == null) {
							int position = getDefinition().getInputNames().indexOf(aggregateKey.getName());
							if (position < 0) {
								throw new ServiceException("JDBC-13", "Can not find the position of the collection key in the statement");
							}
							tableName = getTableName(statement, position);
						}
						if (tableName == null) {
							throw new ServiceException("JDBC-14", "Can not determine the table name for the limiting");
						}
						Map<Object, Integer> counts = new HashMap<Object, Integer>();
						for (ComplexContent parameter : parameters) {
							Object key = parameter.get(aggregateKey.getName());
							// don't track if it doesn't have a value
							if (key != null) {
								if (!counts.containsKey(key)) {
									counts.put(key, 1);
								}
								else {
									counts.put(key, counts.get(key) + 1);
								}
							}
						}
						if (!counts.isEmpty()) {
							String aggregateKeyName = uncamelify(aggregateKey.getName());
							for (Object key : counts.keySet()) {
								PreparedStatement countStatement = connection.prepareStatement("select count(*) from " + tableName + " where " + aggregateKeyName + " = ?");
								countStatement.setObject(1, key);
								ResultSet executeQuery = countStatement.executeQuery();
								if (!executeQuery.next()) {
									throw new ServiceException("JDBC-15", "Can not determine amount of elements in '" + tableName + "' for field " + aggregateKeyName + " = " + key);
								}
								long number = executeQuery.getLong(1);
								if (counts.get(key) + number > limit) {
									throw new ServiceException("JDBC-16", "Too many elements for table '" + tableName + "' field '" + aggregateKeyName + "' value '" + key + "', currently " + number + " in database and " + counts.get(key) + " would be added (> " + limit + ")");
								}
							}
						}
					}
					if (definition.getChangeTracker() != null && trackChanges) {
						for (Element<?> element : TypeUtils.getAllChildren(definition.getParameters())) {
							Value<Boolean> property = element.getProperty(PrimaryKeyProperty.getInstance());
							if (property != null && property.getValue()) {
								primaryKey = element;
								break;
							}
						}
						if (primaryKey == null) {
							throw new ServiceException("JDBC-8", "Can only track changes if the input object has a primary key");
						}
						primaryKeyName = uncamelify(primaryKey.getName());
						if (tableName == null) {
							int position = getDefinition().getInputNames().indexOf(primaryKey.getName());
							if (position < 0) {
								throw new ServiceException("JDBC-9", "Can not find the position of the primary key in the statement");
							}
							tableName = getTableName(statement, position);
						}
						if (tableName == null) {
							throw new ServiceException("JDBC-10", "Can not determine the table name for the change tracking");
						}
						selectBuilder = new StringBuilder();
						selectBuilder.append("select * from " + tableName + " where");
						boolean first = true;
						primaryKeys = new ArrayList<Object>();
						for (ComplexContent parameter : parameters) {
							Object key = parameter.get(primaryKey.getName());
							if (key == null) {
								throw new ServiceException("JDBC-11", "No primary key present in the input");
							}
							primaryKeys.add(key);
							if (first) {
								first = false;
							}
							else {
								selectBuilder.append(" or");
							}
							selectBuilder.append(" " + primaryKeyName + " = ?");
						}
						missing = new ArrayList<Object>(primaryKeys);
						// if it is an insert statement, they should all be new, otherwise select them so we can track the changes
						if (!preparedSql.trim().toLowerCase().startsWith("insert")) {
							PreparedStatement selectAll = connection.prepareStatement(selectBuilder.toString());
							for (int i = 0; i < primaryKeys.size(); i++) {
								selectAll.setObject(i + 1, primaryKeys.get(i));
							}
							ResultSet executeQuery = selectAll.executeQuery();
							original = new HashMap<Object, Map<String, Object>>();
							ResultSetMetaData metaData = executeQuery.getMetaData();
							int columnCount = metaData.getColumnCount();
							while (executeQuery.next()) {
								Map<String, Object> current = new HashMap<String, Object>();
								for (int i = 1; i <= columnCount; i++) {
									current.put(metaData.getColumnLabel(i), executeQuery.getObject(i));
								}
								original.put(current.get(primaryKeyName), current);
							}
							missing.removeAll(original.keySet());
						}
					}
					
					MetricTimer timer = metrics == null ? null : metrics.start(METRIC_EXECUTION_TIME);
					int[] executeBatch = batchInputAdded ? statement.executeBatch() : new int[] { statement.executeUpdate() };
					int total = 0;
					for (int amount : executeBatch) {
						total += amount;
					}
					output.set(JDBCService.ROW_COUNT, total);
					Long executionTime = timer == null ? null : timer.stop();
					if (debug != null) {
						debug.setExecutionDuration(executionTime);
						debug.setOutputAmount(total);
					}
					
					if (definition.getChangeTracker() != null && trackChanges) {
						PreparedStatement selectAll = connection.prepareStatement(selectBuilder.toString());
						for (int i = 0; i < primaryKeys.size(); i++) {
							selectAll.setObject(i + 1, primaryKeys.get(i));
						}
						ResultSet executeQuery = selectAll.executeQuery();
						Map<Object, Map<String, Object>> updated = new HashMap<Object, Map<String, Object>>();
						ResultSetMetaData metaData = executeQuery.getMetaData();
						int columnCount = metaData.getColumnCount();
						while (executeQuery.next()) {
							Map<String, Object> current = new HashMap<String, Object>();
							for (int i = 1; i <= columnCount; i++) {
								current.put(metaData.getColumnLabel(i), executeQuery.getObject(i));
							}
							updated.put(current.get(primaryKeyName), current);
						}
						List<Object> newMissing = new ArrayList<Object>(primaryKeys);
						newMissing.removeAll(updated.keySet());
						// don't take into account the ones that were already missing
						newMissing.removeAll(missing);

						List<ChangeSet> changesets = new ArrayList<ChangeSet>();
						
						// first the deleted
						for (Object key : newMissing) {
							changesets.add(new ChangeSetImpl(key, ChangeType.DELETE, original.get(key), null));
						}
						
						// the newly created
						for (Object key : missing) {
							if (updated.containsKey(key)) {
								Map<String, Object> initial = updated.get(key);
								Iterator<String> iterator = initial.keySet().iterator();
								// remove the empty ones
								while (iterator.hasNext()) {
									if (initial.get(iterator.next()) == null) {
										iterator.remove();
									}
								}
								changesets.add(new ChangeSetImpl(key, ChangeType.INSERT, null, updated.get(key)));
								updated.remove(key);
							}
						}

						// compare the old for updates
						for (Object key : updated.keySet()) {
							Map<String, Object> current = updated.get(key);
							Map<String, Object> old = original.get(key);
							for (String name : old.keySet()) {
								Object currentValue = current.get(name);
								Object oldValue = old.get(name);
								// if it is unchanged, remove it from the new mapping
								if (currentValue == null && oldValue == null || currentValue != null && currentValue.equals(oldValue)) {
									current.remove(name);
								}
							}
							// if something was updated, add it to the diff
							if (!current.isEmpty()) {
								changesets.add(new ChangeSetImpl(key, ChangeType.UPDATE, old, current));
							}
						}
						if (!changesets.isEmpty()) {
							definition.getChangeTracker().track(connectionId, transactionId, tableName, changesets);
						}
					}
				}
				else {
					ComplexType resultType = (ComplexType) getDefinition().getOutput().get(JDBCService.RESULTS).getType();
					MetricTimer timer = metrics == null ? null : metrics.start(METRIC_EXECUTION_TIME);
					// if we have an offset/limit but it was not fixed natively through the dialect, do it programmatically
					if (!nativeLimit && limit != null) {
						statement.setMaxRows(offset != null ? offset + limit : limit);
					}
					ResultSet executeQuery = statement.executeQuery();
					Long executionTime = timer == null ? null : timer.stop();
					if (debug != null) {
						debug.setExecutionDuration(executionTime);
					}
					if (lazy) {
						if (!executeQuery.next()) {
							executeQuery.close();
							output.set(JDBCService.ROW_COUNT, 0);
						}
						else {
							output.set(JDBCService.RESULTS, new ResultSetWithType(executeQuery, resultType, true));
							// the size is unknown
							output.set(JDBCService.ROW_COUNT, -1);
						}
					}
					else {
						timer = metrics == null ? null : metrics.start(METRIC_MAP_TIME);
						int recordCounter = 0;
						int index = 0;
						try {
							while (executeQuery.next()) {
								// if we don't have a native (dialect) limit but we did set an offset, do it programmatically
								if (!nativeLimit && offset != null) {
									recordCounter++;
									if (recordCounter < offset) {
										continue;
									}
								}
								ComplexContent result;
								try {
									result = ResultSetCollectionHandler.convert(executeQuery, resultType);
								}
								catch (IllegalArgumentException e) {
									throw new ServiceException("JDBC-4", "Invalid type", e);
								}
								output.set(JDBCService.RESULTS + "[" + index++ + "]", result);
							}
						}
						finally {
							executeQuery.close();
						}
						output.set(JDBCService.ROW_COUNT, index);
						Long mappingTime = timer == null ? null : timer.stop();
						if (debug != null) {
							debug.setMappingDuration(mappingTime);
							debug.setOutputAmount(index);
						}
					}
				}
				
				if (generatedColumn != null) {
					ResultSet generatedKeys = statement.getGeneratedKeys();
					List<Long> generated = new ArrayList<Long>();
					while (generatedKeys.next()) {
						generated.add(generatedKeys.getLong(1));
					}
					output.set(JDBCService.GENERATED_KEYS, generated);
				}
				if (definition.getValidateOutput() != null && definition.getValidateOutput()) {
					Validator validator = output.getType().createValidator();
					List<? extends Validation<?>> validations = validator.validate(output);
					for (Validation<?> validation : validations) {
						if (validation.getSeverity() == Severity.CRITICAL || validation.getSeverity() == Severity.ERROR) {
							throw new ServiceException("JDBC-6", "The output is not valid: " + validations);
						}
					}
				}
				return output;
			}
			finally {
				if (lazy) {
					executionContext.getTransactionContext().add(transactionId, new TransactionCloseable(statement));
				}
				else {
					statement.close();
				}
			}
		}	
		catch (SQLException e) {
			while (e.getNextException() != null) {
				e = e.getNextException();
			}
			throw new ServiceException(e);
		}
		finally {
			// if the pool is set to auto commit and a connection was created, close it so it is released to the pool again
			if (dataSourceProvider.isAutoCommit() && connection != null) {
				try {
					connection.close();
				}
				catch (SQLException e) {
					// do nothing
				}
			}
			if (debug != null) {
				ServiceRuntime runtime = ServiceRuntime.getRuntime();
				if (runtime != null && runtime.getRuntimeTracker() != null) {
					runtime.getRuntimeTracker().report(debug);
				}
			}
		}
	}

	private String getTableName(PreparedStatement statement, int position) {
		String tableName = null;
		try {
			// 1-based
			ResultSetMetaData metaData = statement.getMetaData();
			if (metaData != null) {
				tableName = metaData.getTableName(position + 1);
			}
		}
		catch (Exception e) {
			logger.warn("Can not get table name from statement", e);
		}
		// best effort
		if (tableName == null) {
			tableName = ValueUtils.getValue(CollectionNameProperty.getInstance(), definition.getParameters().getProperties());
			if (tableName == null) {
				tableName = definition.getParameters().getName();
			}
			if (tableName != null) {
				tableName = uncamelify(tableName);
			}
		}
		return tableName;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static Collection<ComplexContent> toContentCollection(Object object) {
		List<ComplexContent> result = new ArrayList<ComplexContent>();
		if (object instanceof Collection) {
			for (Object child : (Collection) object) {
				if (!(child instanceof ComplexContent)) {
					child = ComplexContentWrapperFactory.getInstance().getWrapper().wrap(child);
				}
				result.add((ComplexContent) child);
			}
		}
		else {
			if (!(object instanceof ComplexContent)) {
				object = ComplexContentWrapperFactory.getInstance().getWrapper().wrap(object);
			}
			result.add((ComplexContent) object);
		}
		return result;
	}
	
	@Override
	public JDBCService getDefinition() {
		return definition;
	}

	public static class ConnectionTransactionable implements Transactionable {

		private Connection connection;
		private String id;
		private boolean closed = false;
		
		public ConnectionTransactionable(String id, Connection connection) {
			this.id = id;
			this.connection = connection;
		}

		@Override
		public void commit() {
			if (!closed) {
				try {
					try {
						connection.commit();
					}
					finally {
						connection.close();
						closed = true;
					}
				}
				catch (SQLException e) {
					throw new RuntimeException(e);
				}
			}
		}

		@Override
		public String getId() {
			return id;
		}

		@Override
		public void rollback() {
			if (!closed) {
				try {
					try {
						connection.rollback();
					}
					finally {
						connection.close();
						closed = true;
					}
				}
				catch (SQLException e) {
					throw new RuntimeException(e);
				}
			}
		}

		@Override
		public void start() {
			// do nothing, it is autostarted
		}

		public Connection getConnection() {
			return connection;
		}

		@Override
		protected void finalize() throws Throwable {
			super.finalize();
			if (!closed) {
				rollback();
			}
		}
	}
	
	public static String uncamelify(String string) {
		StringBuilder builder = new StringBuilder();
		boolean previousUpper = false;
		for (int i = 0; i < string.length(); i++) {
			String substring = string.substring(i, i + 1);
			if (substring.equals(substring.toLowerCase()) || i == 0) {
				previousUpper = !substring.equals(substring.toLowerCase());
				builder.append(substring.toLowerCase());
			}
			else {
				// if it is not preceded by a "_" or another capitilized
				if (!string.substring(i - 1, i).equals("_") && !previousUpper) {
					builder.append("_");
				}
				previousUpper = true;
				builder.append(substring.toLowerCase());
			}
		}
		return builder.toString();
	}
}
