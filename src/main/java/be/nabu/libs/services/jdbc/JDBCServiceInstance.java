package be.nabu.libs.services.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.libs.converter.ConverterFactory;
import be.nabu.libs.converter.api.Converter;
import be.nabu.libs.metrics.api.MetricInstance;
import be.nabu.libs.metrics.api.MetricTimer;
import be.nabu.libs.property.ValueUtils;
import be.nabu.libs.services.ServiceRuntime;
import be.nabu.libs.services.api.ExecutionContext;
import be.nabu.libs.services.api.ServiceException;
import be.nabu.libs.services.api.ServiceInstance;
import be.nabu.libs.services.api.Transactionable;
import be.nabu.libs.services.jdbc.api.DataSourceWithDialectProviderArtifact;
import be.nabu.libs.types.CollectionHandlerFactory;
import be.nabu.libs.types.TypeUtils;
import be.nabu.libs.types.api.CollectionHandlerProvider;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.api.SimpleType;
import be.nabu.libs.types.api.Unmarshallable;
import be.nabu.libs.types.properties.ActualTypeProperty;
import be.nabu.libs.types.properties.FormatProperty;
import be.nabu.libs.types.properties.TimezoneProperty;
import be.nabu.libs.types.utils.DateUtils;
import be.nabu.libs.types.utils.DateUtils.Granularity;
import be.nabu.libs.validator.api.Validation;
import be.nabu.libs.validator.api.Validator;
import be.nabu.libs.validator.api.ValidationMessage.Severity;

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
				parameters = isBatch 
					? (Collection<ComplexContent>) content.get(JDBCService.PARAMETERS)
					: Arrays.asList((ComplexContent) content.get(JDBCService.PARAMETERS));
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
								// check if there is some known conversion logic
								// anything else is given straight to the JDBC adapter in the hopes that it can handle the type
								if (value != null) {
									boolean isList = element.getType().isList(element.getProperties());
									// IMPORTANT: this bit of code assumes the database supports the setting of an object array instead of a single value
									// some databases support this, some don't so it depends on the database and more specifically its driver
									if (isList) {
										Integer sqlType = getSQLType(simpleType.getInstanceClass());
										CollectionHandlerProvider provider = CollectionHandlerFactory.getInstance().getHandler().getHandler(value.getClass());
										if (provider == null) {
											throw new RuntimeException("Unknown collection type: " + value.getClass());
										}
										Collection collection = provider.getAsCollection(value);
										// most databases that do support object arrays don't support untyped object arrays, so if we can't deduce the sql type, this will very likely fail
										if (sqlType == null) {
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
												statement.setNull(index++, sqlType);
											}
											else {
												// we can either create an array that is specific to the database
												// note: even using this construct you can not do "where a in (?)" but instead need to do "where a = any(?)" (at least for postgresql)
												java.sql.Array array = connection.createArrayOf(getSQLType(sqlType), collection.toArray());
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
										for (Class<?> originalType : getDefinition().getTypeConversions().keySet()) {
											if (originalType.isAssignableFrom(value.getClass())) {
												value = converter.convert(value, getDefinition().getTypeConversions().get(originalType));
												break;
											}
										}
										statement.setObject(index++, value);
									}
								}
								else {
									Integer sqlType = getSQLType(simpleType.getInstanceClass());
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

	/**
	 * Currently this is mostly for array generation, and as the documentation of Connection states, the value is generally database-specific
	 * In theory this should move to the dialect but for now we only use standard types which are (hopefully) the same across databases
	 */
	public static String getSQLType(Integer integer) {
		switch(integer) {
			case Types.VARBINARY: return "varbinary";
			case Types.INTEGER: return "integer";
			case Types.BIGINT: return "bigint";
			case Types.DOUBLE: return "double";
			case Types.FLOAT: return "float";
			case Types.SMALLINT: return "smallint";
			case Types.BOOLEAN: return "boolean";
			case Types.TIMESTAMP: return "timestamp";
			case Types.OTHER: return "other";
			default: return "varchar";
		}
	}
	
	public static Integer getPredefinedSQLType(Class<?> instanceClass) {
		if (String.class.isAssignableFrom(instanceClass) || char[].class.isAssignableFrom(instanceClass)) {
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
	private Integer getSQLType(Class<?> instanceClass) {
		if (getDefinition().getTypeConversions().containsKey(instanceClass)) {
			instanceClass = getDefinition().getTypeConversions().get(instanceClass);
		}
		return getPredefinedSQLType(instanceClass);
	}

	@Override
	public JDBCService getDefinition() {
		return definition;
	}

	private static class ConnectionTransactionable implements Transactionable {

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
}
