package be.nabu.libs.services.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
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
import be.nabu.libs.types.TypeUtils;
import be.nabu.libs.types.api.CollectionHandlerProvider;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.api.SimpleType;
import be.nabu.libs.types.api.Unmarshallable;
import be.nabu.libs.types.properties.ActualTypeProperty;
import be.nabu.libs.types.properties.CollectionNameProperty;
import be.nabu.libs.types.properties.FormatProperty;
import be.nabu.libs.types.properties.PrimaryKeyProperty;
import be.nabu.libs.types.properties.TimezoneProperty;
import be.nabu.libs.types.utils.DateUtils;
import be.nabu.libs.types.utils.DateUtils.Granularity;
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
		JDBCDebugInformation debug = executionContext.isDebug() ? new JDBCDebugInformation() : null;
		// get the connection id, you can override this at runtime
		String connectionId = content == null ? null : (String) content.get(JDBCService.CONNECTION);
		if (connectionId == null) {
			connectionId = getDefinition().getConnectionId();
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
			String preparedSql = getDefinition().getPreparedSql(dataSourceProvider.getDialect());
			
			if (preparedSql == null) {
				throw new ServiceException("JDBC-7", "No sql found for: " + definition.getId() + ", expecting rewritten: " + definition.getSql());
			}

			Integer offset = content == null ? null : (Integer) content.get(JDBCService.OFFSET);
			Integer limit = content == null ? null : (Integer) content.get(JDBCService.LIMIT);
			boolean nativeLimit = false;
			if (offset != null || limit != null) {
				if (dataSourceProvider.getDialect() != null) {
					String limitedSql = dataSourceProvider.getDialect().limit(preparedSql, offset, limit);
					if (limitedSql != null) {
						preparedSql = limitedSql;
						nativeLimit = true;
					}
				}
			}
			
			// if it's not a batch, check for "$" variables to replace
			if (!isBatch && parameters != null && parameters.size() > 0) {
				Pattern pattern = Pattern.compile("\\$[\\w]+");
				Matcher matcher = pattern.matcher(preparedSql);
				ComplexContent parameter = parameters.iterator().next();
				while (matcher.find()) {
					String name = matcher.group().substring(1);
					Object value = parameter.get(name);
					preparedSql = preparedSql.replaceAll(Pattern.quote(matcher.group()), value == null ? "null" : converter.convert(value, String.class));
				}
			}
			if (debug != null) {
				debug.setSql(preparedSql);
			}
			String generatedColumn = definition.getGeneratedColumn();
			PreparedStatement statement = generatedColumn != null
				? connection.prepareStatement(preparedSql, new String[] { generatedColumn })
				: connection.prepareStatement(preparedSql);
			try {
				if (parameters != null) {
					if (debug != null) {
						debug.setInputAmount(parameters.size());
					}
					for (ComplexContent parameter : parameters) {
						int index = 1;
						for (String inputName : getDefinition().getInputNames()) {
							Element<?> element = parameter.getType().get(inputName);
							if (element == null) {
								throw new ServiceException("JDBC-2", "Can not determine the metadata for the field: " + inputName, inputName);
							}
							else if (!(element.getType() instanceof SimpleType)) {
								throw new ServiceException("JDBC-3", "The field has a non-simple type: " + inputName, inputName);
							}
							SimpleType<?> simpleType = (SimpleType<?>) element.getType();
							Object value = parameter.get(inputName);
							logger.trace("Setting parameter '{}' = '{}'", inputName, value);
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
											statement.setNull(index++, Types.DATE);
										}
										else {
											statement.setDate(index++, new java.sql.Date(((Date) value).getTime()), calendar);
										}
									break;
									case TIME:
										if (value == null) {
											statement.setNull(index++, Types.TIME);	
										}
										else {
											statement.setTime(index++, new java.sql.Time(((Date) value).getTime()), calendar);
										}
									break;
									case TIMESTAMP:
										if (value == null) {
											statement.setNull(index++, Types.TIMESTAMP);
										}
										else {
											statement.setTimestamp(index++, new java.sql.Timestamp(((Date) value).getTime()), calendar);
										}
									break;
									default:
										throw new ServiceException("JDBC-4", "Unknown date granularity: " + granularity, granularity);
								}
							}
							else {
								SQLDialect dialect = dataSourceProvider.getDialect();
								if (dialect == null) {
									dialect = new DefaultDialect();
								}
								// check if there is some known conversion logic
								// anything else is given straight to the JDBC adapter in the hopes that it can handle the type
								if (value != null) {
									boolean isList = element.getType().isList(element.getProperties());
									// IMPORTANT: this bit of code assumes the database supports the setting of an object array instead of a single value
									// some databases support this, some don't so it depends on the database and more specifically its driver
									if (isList) {
										String sqlTypeName = dialect.getSQLName(simpleType.getInstanceClass());
										CollectionHandlerProvider provider = CollectionHandlerFactory.getInstance().getHandler().getHandler(value.getClass());
										if (provider == null) {
											throw new RuntimeException("Unknown collection type: " + value.getClass());
										}
										Collection collection = provider.getAsCollection(value);
										// most databases that do support object arrays don't support untyped object arrays, so if we can't deduce the sql type, this will very likely fail
										if (sqlTypeName == null) {
											logger.warn("Could not map instance class to native SQL type: {}", simpleType.getInstanceClass());
											if (collection.isEmpty()) {
												statement.setObject(index++, null);		
											}
											else {
												statement.setObject(index++, collection.toArray());
											}
										}
										else {
											if (collection.isEmpty()) {
												statement.setNull(index++, dialect.getSQLType(simpleType.getInstanceClass()), sqlTypeName);
											}
											else {
												// we can either create an array that is specific to the database
												// note: even using this construct you can not do "where a in (?)" but instead need to do "where a = any(?)" (at least for postgresql)
												java.sql.Array array = connection.createArrayOf(sqlTypeName, collection.toArray());
												statement.setObject(index++, array, Types.ARRAY);
												
												// or simply pass an object array and hope it works...
												// note: this does NOT work on postgresql
	//											Object first = collection.iterator().next();
	//											Object newInstance = Array.newInstance(first.getClass(), collection.size());
	//											statement.setObject(index++, collection.toArray((Object[]) newInstance), sqlType);
											}
										}
									}
									else {
										Class<?> targetType = dialect.getTargetClass(value.getClass());
										if (!targetType.equals(value.getClass())) {
											value = converter.convert(value, targetType);
											if (value == null) {
												throw new RuntimeException("Could not convert the value to: " + targetType);
											}
										}
										statement.setObject(index++, value);
									}
								}
								else {
									Integer sqlType = dialect.getSQLType(simpleType.getInstanceClass());
									// could not perform a mapping, just pass it to the driver and hope it can figure it out
									if (sqlType == null) {
										logger.warn("Could not map instance class to native SQL type: {}", simpleType.getInstanceClass());
										statement.setObject(index++, null);	
									}
									else {
										statement.setNull(index++, sqlType);
									}
								}
							}
						}
						if (isBatch) {
							statement.addBatch();
						}
					}
				}
				else if (type.iterator().hasNext()) {
					throw new IllegalArgumentException("Missing input parameters");
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
						int position = getDefinition().getInputNames().indexOf(primaryKey.getName());
						if (position < 0) {
							throw new ServiceException("JDBC-9", "Can not find the position of the primary key in the statement");
						}
						try {
							// 1-based
							tableName = statement.getMetaData().getTableName(position + 1);
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
					int[] executeBatch = statement.executeBatch();
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
					timer = metrics == null ? null : metrics.start(METRIC_MAP_TIME);
					int recordCounter = 0;
					int index = 0;
					while (executeQuery.next()) {
						// if we don't have a native (dialect) limit but we did set an offset, do it programmatically
						if (!nativeLimit && offset != null) {
							recordCounter++;
							if (recordCounter < offset) {
								continue;
							}
						}
						ComplexContent result = resultType.newInstance();
						int column = 1;
						for (Element<?> child : TypeUtils.getAllChildren(resultType)) {
							SimpleType<?> simpleType = (SimpleType<?>) child.getType();
							Object value;
							if (Date.class.isAssignableFrom(simpleType.getInstanceClass())) {
								String format = ValueUtils.getValue(FormatProperty.getInstance(), child.getProperties());
								Granularity granularity = format == null ? Granularity.TIMESTAMP : DateUtils.getGranularity(format);
								TimeZone timezone = ValueUtils.getValue(TimezoneProperty.getInstance(), child.getProperties());
								Calendar calendar = Calendar.getInstance(timezone);
								switch(granularity) {
									case DATE:
										value = executeQuery.getDate(column++, calendar);
									break;
									case TIME:
										value = executeQuery.getTime(column++, calendar);
									break;
									case TIMESTAMP:
										value = executeQuery.getTimestamp(column++, calendar);
									break;
									default:
										throw new ServiceException("JDBC-4", "Unknown date granularity: " + granularity, granularity);
								}
							}
							else {
								value = executeQuery.getObject(column++);
							}
							// conversion should be handled by the result instance
							result.set(child.getName(), value);
						}
						output.set(JDBCService.RESULTS + "[" + index++ + "]", result);
					}
					output.set(JDBCService.ROW_COUNT, index);
					Long mappingTime = timer == null ? null : timer.stop();
					if (debug != null) {
						debug.setMappingDuration(mappingTime);
						debug.setOutputAmount(index);
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
				statement.close();
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
