package be.nabu.libs.services.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.eai.repository.EAIResourceRepository;
import be.nabu.eai.repository.api.Repository;
import be.nabu.eai.repository.util.SystemPrincipal;
import be.nabu.libs.artifacts.api.ExternalDependency;
import be.nabu.libs.artifacts.api.ExternalDependencyArtifact;
import be.nabu.libs.converter.ConverterFactory;
import be.nabu.libs.converter.api.Converter;
import be.nabu.libs.metrics.api.MetricInstance;
import be.nabu.libs.metrics.api.MetricTimer;
import be.nabu.libs.property.ValueUtils;
import be.nabu.libs.property.api.Value;
import be.nabu.libs.services.ServiceRuntime;
import be.nabu.libs.services.TransactionCloseable;
import be.nabu.libs.services.api.ExecutionContext;
import be.nabu.libs.services.api.Service;
import be.nabu.libs.services.api.ServiceException;
import be.nabu.libs.services.api.ServiceInstance;
import be.nabu.libs.services.api.Transactionable;
import be.nabu.libs.services.jdbc.api.ChangeSet;
import be.nabu.libs.services.jdbc.api.ChangeTracker;
import be.nabu.libs.services.jdbc.api.ChangeType;
import be.nabu.libs.services.jdbc.api.DataSourceWithAffixes;
import be.nabu.libs.services.jdbc.api.DataSourceWithTranslator;
import be.nabu.libs.services.jdbc.api.DataSourceWithAffixes.AffixMapping;
import be.nabu.libs.services.jdbc.api.DataSourceWithDialectProviderArtifact;
import be.nabu.libs.services.jdbc.api.JDBCTranslator;
import be.nabu.libs.services.jdbc.api.JDBCTranslator.Translation;
import be.nabu.libs.services.pojo.POJOUtils;
import be.nabu.libs.tracer.api.DatabaseRequestTrace;
import be.nabu.libs.tracer.api.DatabaseRequestTracer;
import be.nabu.libs.tracer.api.Trace;
import be.nabu.libs.tracer.api.TracerProvider;
import be.nabu.libs.tracer.impl.TracerFactory;
import be.nabu.libs.services.jdbc.api.SQLDialect;
import be.nabu.libs.types.BaseTypeInstance;
import be.nabu.libs.types.CollectionHandlerFactory;
import be.nabu.libs.types.ComplexContentWrapperFactory;
import be.nabu.libs.types.DefinedTypeResolverFactory;
import be.nabu.libs.types.SimpleTypeWrapperFactory;
import be.nabu.libs.types.TypeConverterFactory;
import be.nabu.libs.types.TypeUtils;
import be.nabu.libs.types.api.CollectionHandlerProvider;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.DefinedSimpleType;
import be.nabu.libs.types.api.DefinedType;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.api.SimpleType;
import be.nabu.libs.types.api.TypedKeyValuePair;
import be.nabu.libs.types.base.SimpleElementImpl;
import be.nabu.libs.types.java.BeanResolver;
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
	public static final String METRIC_COUNT_TIME = "totalCountExecutionTime";
	public static final Integer PREPARED_STATEMENT_ARRAY_SIZE = Integer.parseInt(System.getProperty("be.nabu.libs.services.jdbc.preparedStatementArraySize", "10"));
	
	private JDBCService definition;
	private Converter converter = ConverterFactory.getInstance().getConverter();
	private Logger logger = LoggerFactory.getLogger(getClass());

	JDBCServiceInstance(JDBCService definition) {
		this.definition = definition;
	}
	
	public static boolean isNullCheck(String sql, int index) {
		if (sql == null) {
			return false;
		}
		
		// the index is 1-based but we want to wipe the index itself as well because we are interested in what is behind it
		for (int i = 0; i < index; i++) {
			int indexOf = sql.indexOf(':');
			if (indexOf < 0) {
				return false;
			}
			sql = sql.substring(indexOf + 1);
		}
		// remove the name of the parameter
		sql = sql.replaceFirst("^[\\w]+?\\b", "");
		return sql.matches("(?is)^[\\s]+is[\\s]+null\\b.*") || sql.matches("(?is)^[\\s]+is[\\s]+not[\\s]+null\\b.*");
	}
	
	public static ChangeTracker getAsChangeTracker(Repository repository, String id) {
		if (id != null && !id.trim().isEmpty()) {
			Service changeTracker = (Service) repository.resolve(id);
			if (changeTracker == null) {
				throw new IllegalArgumentException("Could not find change tracker: " + id);
			}
			return POJOUtils.newProxy(ChangeTracker.class, repository, SystemPrincipal.ROOT, changeTracker);
		}
		return null;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public ComplexContent execute(ExecutionContext executionContext, ComplexContent content) throws ServiceException {
		if (definition.getValidateInput() != null && definition.getValidateInput()) {
			Validator validator = content.getType().createValidator();
			List<? extends Validation<?>> validations = validator.validate(content);
			for (Validation<?> validation : validations) {
				if (validation.getSeverity() == Severity.CRITICAL || validation.getSeverity() == Severity.ERROR) {
					ServiceException serviceException = new ServiceException("JDBC-5", "The input is not valid: " + validations);
					serviceException.setValidations(validations);
					throw serviceException;
				}
			}
		}
		MetricInstance metrics = executionContext.getMetricInstance(definition.getId());
		
		// if we pass in a change tracker id and trackChanges is not explicitly set, we set it explicitly
		String changeTrackerId = content == null ? null : (String) content.get(JDBCService.CHANGE_TRACKER);
		ChangeTracker changeTracker = null;
		if (changeTrackerId != null) {
			changeTracker = getAsChangeTracker(EAIResourceRepository.getInstance(), changeTrackerId);
		}
		if (changeTracker == null) {
			changeTracker = definition.getChangeTracker();
		}
		
		boolean trackChanges = content == null || content.get(JDBCService.TRACK_CHANGES) == null || (Boolean) content.get(JDBCService.TRACK_CHANGES);
		boolean lazy = content != null && content.get(JDBCService.LAZY) != null && (Boolean) content.get(JDBCService.LAZY);
		// for postgres (and perhaps others?) actually counting results is very time consuming
		// we had a query on a million rows that had a cost of 450, the total count (on the id field) was 150.000 cost
		// the total count on the * (which is _not_ expanded by default in postgres and actually faster) was 50.000 cost which is a lot better than the previous count but still massively slower than the actual query
		// we can use the query plan to get an estimate of the amount of rows though, in some early tests we had this:
		// a table with 4925 rows (using regular count) returned 4905 rows in an explain plan count
		// a result set within that table yielding 218 rows (regular count) had exactly 218 rows in explain plan
		// a table with 808593 rows, if we do an explain on it, we only get 335967 rows, even after refreshing the statistics
		// a resultset of 10097 within that table has 9407 in explain
		// a resultset of 153917 (which takes 5 seconds to calculate) has 63755 in explain (instantaneous)
		boolean useQueryPlanTotalCount = true;
		
		// always report the debug information?
		JDBCDebugInformation debug = executionContext.isDebug() || true ? new JDBCDebugInformation() : null;
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
		
		List<AffixMapping> affixes = dataSourceProvider instanceof DataSourceWithAffixes ? ((DataSourceWithAffixes) dataSourceProvider).getAffixes() : null;
		
		Object affixResult = content == null ? null : content.get(JDBCService.AFFIX);
		if (affixResult != null) {
			if (affixes == null) {
				affixes = new ArrayList<AffixMapping>();
			}
			for (Object single : (Iterable) affixResult) { 
				AffixInput affixInput = single instanceof AffixInput ? (AffixInput) single : TypeUtils.getAsBean((ComplexContent) single, AffixInput.class);
				AffixMapping mapping = new AffixMapping();
				mapping.setAffix(affixInput.getAffix());
				mapping.setTables(affixInput.getTables());
				mapping.setContext(definition.getId());
				affixes.add(0, mapping);
			}
		}

		String originalSql = null, preparedSql = null;
		// if it's not autocommitted, we need to check if there is already a transaction open on this resource for the given transaction id
		Connection connection = null;
		List<Trace> runningTraces = new ArrayList<Trace>();
		DatabaseRequestTracer tracer = null;
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
			
			
			SQLDialect dialect = dataSourceProvider.getDialect();
			if (dialect == null) {
				dialect = new DefaultDialect();
			}
			
			TracerProvider tracerProvider = TracerFactory.getInstance().newTracerProvider();
			ExternalDependency dependency = null;
			if (dataSourceProvider instanceof ExternalDependencyArtifact) {
				List<ExternalDependency> externalDependencies = ((ExternalDependencyArtifact) dataSourceProvider).getExternalDependencies();
				dependency = externalDependencies == null || externalDependencies.isEmpty() ? null : externalDependencies.get(0);
			}
			tracer = tracerProvider.newDatabaseRequestTracer(dataSourceProvider.getId(), dialect.getClass().getSimpleName(), dependency == null ? null : dependency.getEndpoint());
			
			// --------------------- language handler for update ------------------------
			Element<?> languageElement = content == null ? null : content.getType().get("language");
			String language = null;
			if (languageElement != null) {
				language = (String) content.get("language");
			}
			// this combination is only possible for an update at which point we want to actually update translations
			// must have a translator
			if (isBatch && language != null && dataSourceProvider instanceof DataSourceWithTranslator && ((DataSourceWithTranslator) dataSourceProvider).getTranslator() != null
					// if there is no default language or it differs from the one you are updating, we need to put it to translations
					&& (((DataSourceWithTranslator) dataSourceProvider).getDefaultLanguage() == null || !((DataSourceWithTranslator) dataSourceProvider).getDefaultLanguage().equals(language))) {
				
				List<Translation> translations = new ArrayList<Translation>();
				if (parameters != null && !parameters.isEmpty()) {
					Element<?> primaryKey = null;
					String tableName = null;
					
					for (ComplexContent parameter : parameters) {
						if (tableName == null) {
							String collectionProperty = ValueUtils.getValue(CollectionNameProperty.getInstance(), parameter.getType().getProperties());
							if (collectionProperty != null) {
								tableName = uncamelify(collectionProperty);
							}
						}
						if (primaryKey == null) {
							for (Element<?> child : TypeUtils.getAllChildren(parameter.getType())) {
								Value<Boolean> primaryKeyProperty = child.getProperty(PrimaryKeyProperty.getInstance());
								if (primaryKeyProperty != null && primaryKeyProperty.getValue()) {
									if (tableName == null) {
										// if the input is generated and we set a collection name on the primary key, we assume it is for the table
										// for generated inputs, we can't set properties on the root
										Value<String> collectionProperty = child.getProperty(CollectionNameProperty.getInstance());
										if (collectionProperty != null) {
											tableName = uncamelify(collectionProperty.getValue());
										}
									}
									primaryKey = child;
								}
								break;
							}
						}
					}
					if (primaryKey == null) {
						throw new ServiceException("JDBC-18", "Primary key needed to perform auto-translations");
					}
					if (tableName == null) {
						throw new ServiceException("JDBC-14", "Can not determine the table name for translations");
					}
					
					// we want to select the current records to check which fields are actually different
					StringBuilder selectBuilder = new StringBuilder();
					String primaryKeyName = uncamelify(primaryKey.getName());
					selectBuilder.append("select ");
					
					boolean first = true;
					for (Element<?> child : TypeUtils.getAllChildren(definition.getParameters())) {
						if (first) {
							first = false;
						}
						else {
							selectBuilder.append(", ");
						}
						selectBuilder.append(uncamelify(child.getName()));
					}
					
					selectBuilder.append(" from " + tableName + " where ");
					first = true;
					for (@SuppressWarnings("unused") ComplexContent parameter : parameters) {
						if (first) {
							first = false;
						}
						else {
							selectBuilder.append(" or ");
						}
						selectBuilder.append(primaryKeyName + " = ?");
					}
					Map<Object, ComplexContent> originalVersions = new HashMap<Object, ComplexContent>();
					// rewrite sql for escaping, ::uuid additions etc, not entirely sure if the type to rewrite is correct...
					String selectOriginalSql = dialect.rewrite(selectBuilder.toString(), definition.getParameters(), definition.getParameters());
					PreparedStatement selectAll = connection.prepareStatement(selectOriginalSql);
					try {
						int i = 1;
						for (ComplexContent parameter : parameters) {
							Object idObject = parameter.get(primaryKey.getName());
							if (idObject == null) {
								throw new ServiceException("JDBC-11", "No primary key present in the input");
							}
							dialect.setObject(selectAll, primaryKey, i++, idObject, selectBuilder.toString());
//							selectAll.setObject(i++, idObject);
						}
						int counter = 0;
						// we select the original values when you are updating them to see how they differ (and save the changed fields)
						DatabaseRequestTrace trace = tracer.newTrace(definition.getId(), "translation-original-select", selectOriginalSql);
						trace.start();
						runningTraces.add(trace);
						ResultSet executeQuery = selectAll.executeQuery();
						while (executeQuery.next()) {
							ComplexContent result = ResultSetCollectionHandler.convert(executeQuery, definition.getParameters());
							Object idObject = result.get(primaryKey.getName());
							originalVersions.put(idObject, result);
							counter++;
						}
						trace.setRowCount(counter);
						trace.setBatchSize(i - 1);
						trace.stop();
						runningTraces.remove(trace);
					}
					finally {
						selectAll.close();
					}
					
					for (ComplexContent parameter : parameters) {
						Object idObject = parameter.get(primaryKey.getName());
						ComplexContent original = originalVersions.get(idObject);
						if (original == null) {
							throw new ServiceException("JDBC-20", "No original found for translations for: " + tableName + " id " + idObject);
						}
						
						String id = null;
						// if the primary key is a uuid, it is globally unique, no additional identifier necessary
						if (((SimpleType<?>) primaryKey.getType()).getInstanceClass().equals(UUID.class)) {
							id = idObject.toString().replace("-", "");
						}
						else if (idObject instanceof String && idObject.toString().matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}|[0-9a-fA-F]{32}")) {
							id = idObject.toString().replace("-", "");
						}
						else {
							id = tableName + ":" + converter.convert(idObject, String.class);
						}
						
						for (Element<?> child : TypeUtils.getAllChildren(parameter.getType())) {
							if (child.getType() instanceof SimpleType) {
								Class<?> instanceClass = ((SimpleType<?>) child.getType()).getInstanceClass();
								if (String.class.isAssignableFrom(instanceClass) || converter.canConvert(instanceClass, String.class)) {
									Translation translation = new Translation();
									translation.setId(id);
									translation.setName(child.getName());
									Object translatedValue = parameter.get(child.getName());
									Object originalValue = original.get(child.getName());
									
									if (originalValue != null && translatedValue != null && !translatedValue.getClass().isAssignableFrom(originalValue.getClass())) {
										originalValue = converter.convert(originalValue, translatedValue.getClass());
									}
									
									// if the values are the same, we set the translated to null as the original is OK
									if ((translatedValue == null && originalValue == null)
											|| (translatedValue != null && translatedValue.equals(originalValue))) {
										translatedValue = null;
									}
									translation.setTranslation(translatedValue == null || translatedValue instanceof String ? (String) translatedValue : converter.convert(translatedValue, String.class));
									translations.add(translation);
								}
							}
						}
					}
					
					// if not empty, push it
					if (!translations.isEmpty()) {
						((DataSourceWithTranslator) dataSourceProvider).getTranslator().set(connectionId, transactionId, language, translations);
					}
					
				}
				// need primary key in the definition of the object
				// if the primary key is a uuid, that is enough, otherwise we take the table name combined with the id as key
				
				
				// -------------------------------- EARLY RETURN... -------------------------
				ComplexContent output = getDefinition().getOutput().newInstance();
				output.set("rowCount", 0);
				return output;
			}
			
			List<String> orderBys = content == null ? null : (List<String>) content.get(JDBCService.ORDER_BY);
			
			preparedSql = getDefinition().getSql();
			
			if (preparedSql == null) {
				throw new ServiceException("JDBC-7", "No sql found for: " + definition.getId() + ", expecting rewritten: " + definition.getSql());
			}
			else {
				preparedSql = getDefinition().expandSql(preparedSql, orderBys);
			}
			
			originalSql = preparedSql;

			// map the additional properties to a map
			Map<String, Object> additional = new HashMap<String, Object>();
			// TODO: the additional typing is not yet respected when rewriting the query
			Map<String, SimpleType<?>> additionalTypes = new HashMap<String, SimpleType<?>>();
			
			List keyValuePairs = content == null ? null : (List) content.get(JDBCService.PROPERTIES);
			if (keyValuePairs != null) {
				for (Object keyValuePair : keyValuePairs) {
					ComplexContent keyValuePairContent = keyValuePair instanceof ComplexContent ? (ComplexContent) keyValuePair : ComplexContentWrapperFactory.getInstance().getWrapper().wrap(keyValuePair);
					Object typed = TypeConverterFactory.getInstance().getConverter().convert(keyValuePairContent, new BaseTypeInstance(keyValuePairContent.getType()), new BaseTypeInstance((ComplexType) BeanResolver.getInstance().resolve(TypedKeyValuePair.class)));
					String additionalType = typed == null ? null : (String) ((ComplexContent) typed).get("type");
					Object value = keyValuePairContent.get("value");
					
					DefinedSimpleType<?> simpleType = null;
					if (additionalType != null) {
						simpleType = SimpleTypeWrapperFactory.getInstance().getWrapper().getByName(additionalType);
						if (simpleType == null) {
							DefinedType resolve = DefinedTypeResolverFactory.getInstance().getResolver().resolve(additionalType);
							if (resolve instanceof SimpleType) {
								simpleType = (DefinedSimpleType<?>) resolve;
							}
						}
					}
					if (simpleType == null) {
						simpleType = SimpleTypeWrapperFactory.getInstance().getWrapper().wrap(String.class);
					}
					
					if (value != null && !simpleType.getInstanceClass().isAssignableFrom(value.getClass())) {
						Object converted = converter.convert(value, simpleType.getInstanceClass());
						if (converted == null) {
							throw new IllegalArgumentException("Could not convert " + value + " to " + simpleType.getInstanceClass());
						}
						value = converted;
					}
					additional.put((String) keyValuePairContent.get("key"), value);
					additionalTypes.put((String) keyValuePairContent.get("key"), simpleType);
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
				String replacement = value == null || value instanceof String ? (String) value : converter.convert(value, String.class);
				// possible but unlikely to create false positives
				// worst case scenario we do runtime calculation of parameters instead of cached because of this
				if (replacement != null) {
					hasNewVariables |= replacement.contains(":");
				}
				preparedSql = preparedSql.replaceAll(Pattern.quote(matcher.group()), replacement == null ? "" : replacement);
			}
			List<String> inputNames = hasNewVariables ? getDefinition().scanForPreparedVariables(preparedSql) : getDefinition().getInputNames();
			
			// this keeps track of the indexes where we are doing a null check (is null or is not null) on a listable object that is: not null and the dialect does not support arrays
			// in this case we want to set something else
			List<Integer> nullCheckIndexes = new ArrayList<Integer>();
			// keeps track of calculated sizes in case the same parameter is used multiple times
			Map<String, Integer> sizes = new HashMap<String, Integer>(); 
			// if the dialect does not support arrays, rewrite the statement if necessary
			int inputIndex = 1;
			// every time we replace something, it is no longer a variable match, we have to take this into account
			int inputIndexOffset = 0;
			List<String> arrayExplosions = new ArrayList<String>();
			for (String inputName : inputNames) {
				Element<?> element = type.get(inputName);
				if (element != null && !dialect.hasArraySupport(element) && element.getType().isList(element.getProperties())) {
					// if we are performing a null check, don't explode the parameter!
					if (isNullCheck(preparedSql, inputIndex - inputIndexOffset)) {
						nullCheckIndexes.add(inputIndex);
						// we mask the input parameter because if it is used multiple times our next hit (see replace below) may accidently convert this one instead of the next one
						preparedSql = preparedSql.replaceFirst(":" + inputName + "\\b", ">>NULL_CHECK=" + inputName + "<<");
						inputIndexOffset++;
					}
					else {
						// we need to find the largest collection in the input
						Integer maxSize = sizes.get(inputName);
						if (maxSize == null) {
							maxSize = 0;
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
							sizes.put(inputName, maxSize);
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
							for (int i = maxSize; i <= PREPARED_STATEMENT_ARRAY_SIZE - (maxSize % PREPARED_STATEMENT_ARRAY_SIZE); i++) {
								builder.append(", :").append(inputName);
							}
							preparedSql = preparedSql.replaceFirst(":" + inputName + "\\b", ">>ARRAY_EXPLOSION=" + arrayExplosions.size() + "<<");
							inputIndexOffset++;
							arrayExplosions.add(builder.toString());
						}
					}
				}
				inputIndex++;
			}
			if (!arrayExplosions.isEmpty()) {
				for (int i = 0; i < arrayExplosions.size(); i++) {
					preparedSql = preparedSql.replace(">>ARRAY_EXPLOSION=" + i + "<<", arrayExplosions.get(i));
				}
			}
			preparedSql = preparedSql.replaceAll(">>NULL_CHECK=([^<]+)<<", ":$1");
			
			preparedSql = getDefinition().getPreparedSql(dataSourceProvider.getDialect(), preparedSql);
			
			// if you have defined an affix, replace it
//			String affix = content == null ? null : (String) content.get(JDBCService.AFFIX);
			
			// if no affix is given at runtime, check if we have some at design time in the connection artifact
//			if (affix == null && affixes != null) {
//				String match = null;
//				for (AffixMapping mapping : affixes) {
//					if (mapping.getNamespace() == null || definition.getId().startsWith(mapping.getNamespace())) {
//						// if we already have a match, check that the new match is more precise, otherwise we skip it
//						if (match != null) {
//							if (mapping.getNamespace() == null || mapping.getNamespace().length() <= match.length()) {
//								continue;
//							}
//						}
//						affix = mapping.getAffix();
//						match = mapping.getNamespace();
//					}
//				}
//			}
			
//			preparedSql = preparedSql.replace("~", affix == null ? "" : affix);
			
			preparedSql = replaceAffixes(definition, affixes, preparedSql);
			
			Long offset = content == null ? null : (Long) content.get(JDBCService.OFFSET);
			Integer limit = content == null ? null : (Integer) content.get(JDBCService.LIMIT);

			// make sure we do a total count statement without limits & offsets
			boolean includeTotalCount = content == null || content.get(JDBCService.INCLUDE_TOTAL_COUNT) == null ? false : (Boolean) content.get(JDBCService.INCLUDE_TOTAL_COUNT);
			PreparedStatement totalCountStatement = null;
			DatabaseRequestTrace totalCountTrace = null;
			// if we want a total count, check if there is a limit (if not, total count == actual count)
			// also check that it is not lazy cause we won't know the total count then even if not limited
			if (includeTotalCount && (limit != null || lazy)) {
				String totalCountSql = getDefinition().getTotalCountSql(dataSourceProvider.getDialect(), preparedSql);
				if (debug != null) {
					debug.setTotalCountSql(totalCountSql);
				}
				totalCountStatement = connection.prepareStatement(totalCountSql);
				totalCountTrace = tracer.newTrace(definition.getId(), "total-count", totalCountSql);
			}
			
			if (orderBys != null && !orderBys.isEmpty()) {
				preparedSql += " ORDER BY ";
				boolean isFirst = true;
				for (String orderBy : orderBys) {
					String direction = null;
					Boolean nullsFirst = null;
					int nullsFirstIndex = orderBy.toLowerCase().indexOf(" nulls first");
					if (nullsFirstIndex > 0) {
						nullsFirst = true;
						orderBy = orderBy.substring(0, nullsFirstIndex).trim();
					}
					int nullsLastIndex = orderBy.toLowerCase().indexOf(" nulls last");
					if (nullsLastIndex > 0) {
						nullsFirst = false;
						orderBy = orderBy.substring(0, nullsLastIndex).trim();
					}
					int asc = orderBy.toLowerCase().indexOf(" asc");
					if (asc > 0) {
						orderBy = orderBy.substring(0, asc);
						direction = "asc";
					}
					else {
						int desc = orderBy.toLowerCase().indexOf(" desc");
						if (desc > 0) {
							orderBy = orderBy.substring(0, desc);
							direction = "desc";
						}
					}
					boolean fieldFound = false;
					int orderByPosition = 0;
					for (Element<?> element : TypeUtils.getAllChildren((ComplexType) getDefinition().getServiceInterface().getOutputDefinition().get(JDBCService.RESULTS).getType())) {
						orderByPosition++;
						if (element.getName().equals(orderBy)) {
							fieldFound = true;
							break;
						}
					}
					if (!fieldFound) {
						throw new ServiceException("JDBC-17", "Invalid order by field: " + orderBy);
					}
					if (isFirst) {
						isFirst = false;
					}
					else {
						preparedSql += ", ";
					}
					preparedSql += orderByPosition;
					if (direction != null) {
						preparedSql += " " + direction;
					}
					if (nullsFirst != null) {
						if (nullsFirst) {
							preparedSql += " nulls first";
						}
						else {
							preparedSql += " nulls last";
						}
					}
				}
			}
			
			boolean overSelected = content == null || content.get(JDBCService.HAS_NEXT) == null ? false : (Boolean) content.get(JDBCService.HAS_NEXT);
			boolean nativeLimit = false;
			// the limit is for selects, we don't want to accidently limit ddl stuff which is also flagged as not batch
			if (!isBatch && (offset != null || limit != null)) {
				// we add one to the limit because we want to be able to set a boolean if there are more
				if (limit != null && !lazy && overSelected) {
					limit++;
				}
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
						int index = 1, inputNameIndex = 0;
						for (String inputName : inputNames) {
							inputNameIndex++;
							Element<?> element = parameter.getType().get(inputName);
							Object value;
							if (element == null) {
								if (additional.containsKey(inputName)) {
									value = additional.get(inputName);
									// because we lack metadata, we assume it is always a string
									// we could try to magically parse it but that could lead to irritating edge cases
									// we could try to allow you to send objects instead of strings but then we might still need additional metadata about the objects (e.g. date)
									// so we strictly limit it to strings, in that edge case that you need actual types, you need to convert them in sql
									element = new SimpleElementImpl(inputName, additionalTypes.get(inputName), null);
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
								// if it is a null check, we want to set the size of the element, not the actual array
								// because the target dialect does not support arrays
								// in this case it will become something like "5 is null"
								if (nullCheckIndexes.contains(inputNameIndex)) {
									CollectionHandlerProvider provider = CollectionHandlerFactory.getInstance().getHandler().getHandler(value.getClass());
									if (provider == null) {
										throw new RuntimeException("Unknown collection type: " + value.getClass());
									}
									if (totalCountStatement != null) {
										dialect.setObject(totalCountStatement, element, index, provider.getAsCollection(value).size(), preparedSql);
									}
									dialect.setObject(statement, element, index++, provider.getAsCollection(value).size(), preparedSql);
								}
								else {
									CollectionHandlerProvider handler = CollectionHandlerFactory.getInstance().getHandler().getHandler(value.getClass());
									int amount = 0;
									Object last = null;
									for (Object single : handler.getAsIterable(value)) {
										if (totalCountStatement != null) {
											dialect.setObject(totalCountStatement, element, index, single, null);
										}
										dialect.setObject(statement, element, index++, single, preparedSql);
										amount++;
										last = single;
									}
									for (int i = amount; i <= PREPARED_STATEMENT_ARRAY_SIZE - (amount % PREPARED_STATEMENT_ARRAY_SIZE); i++) {
										if (totalCountStatement != null) {
											dialect.setObject(totalCountStatement, element, index, last, null);
										}
										dialect.setObject(statement, element, index++, last, preparedSql);
									}
								}
							}
							else {
								if (totalCountStatement != null) {
									dialect.setObject(totalCountStatement, element, index, value, null);
								}
								dialect.setObject(statement, element, index++, value, preparedSql);
							}
						}
						if (isBatch) {
							statement.addBatch();
							if (totalCountStatement != null) {
								totalCountStatement.addBatch();
							}
							batchInputAdded = true;
						}
					}
				}
				// if there are input names but you did not provide anything, add nulls for everything
				else if (!inputNames.isEmpty()) {
					int index = 1;
					for (String inputName : inputNames) {
						Element<?> element = type.get(inputName);
						if (totalCountStatement != null) {
							dialect.setObject(totalCountStatement, element, index, null, null);
						}
						dialect.setObject(statement, element, index++, null, preparedSql);
					}
					if (isBatch) {
						if (totalCountStatement != null) {
							totalCountStatement.addBatch();
						}
						statement.addBatch();
						batchInputAdded = true;
					}
				}
				ComplexContent output = getDefinition().getOutput().newInstance();
				if (isBatch) {
					Element<?> primaryKey = null;
					List<Object> primaryKeys = null;
					List<Element<?>> primaryKeyTypes = null;
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
							if (tableName == null && aggregate != null && definition.isInputGenerated()) {
								Value<String> property = element.getProperty(CollectionNameProperty.getInstance());
								if (property != null) {
									tableName = uncamelify(property.getValue());
								}
							}
							if (definition.isInputGenerated()) {
								Value<Boolean> primaryKeyProperty = element.getProperty(PrimaryKeyProperty.getInstance());
								if (primaryKeyProperty != null && primaryKeyProperty.getValue()) {
									// if the input is generated and we set a collection name on the primary key, we assume it is for the table
									// for generated inputs, we can't set properties on the root
									Value<String> collectionProperty = element.getProperty(CollectionNameProperty.getInstance());
									if (collectionProperty != null) {
										tableName = uncamelify(collectionProperty.getValue());
									}
								}
								primaryKey = element;
							}
							// composite is stronger where the part can not exist without the whole
							// with aggregation both can exist separately but if you are limiting it explicitly, we can still check the amount
							if (aggregate != null && (aggregate.getValue().equals("composite") || aggregate.getValue().equals("aggregate"))) {
								// if we already have an aggregate key, only overwrite it if we have a composite relation (is stricter)
								aggregateKey = aggregateKey == null || aggregate.getValue() == null || aggregate.getValue().equals("composite") ? element : aggregateKey;
							}
						}
						if (aggregateKey == null) {
							throw new ServiceException("JDBC-12", "Can only limit inserts if a composite or aggregate aggregation is found");
						}
						if (tableName == null) {
							int position = getDefinition().getInputNames().indexOf(aggregateKey.getName());
							if (position < 0) {
								throw new ServiceException("JDBC-13", "Can not find the position of the aggregation key in the statement");
							}
							tableName = getTableName(statement, position);
						}
						if (tableName == null) {
							throw new ServiceException("JDBC-21", "Can not determine the table name for the limiting");
						}
						String aggregateKeyName = uncamelify(aggregateKey.getName());
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
								if (counts.get(key) > limit) {
									throw new ServiceException("JDBC-16", "Too many elements for table '" + tableName + "' field '" + aggregateKeyName + "' value '" + key + "', " + counts.get(key) + " would be added (> " + limit + ")");
								}
							}
						}
						if (!counts.isEmpty()) {
							for (Object key : counts.keySet()) {
								String countQuery = "select count(*) from " + tableName + " where " + aggregateKeyName + " = ?";
								DatabaseRequestTrace countTrace = tracer.newTrace(definition.getId(), "count-query-" + tableName + "-" + aggregateKeyName, countQuery);
								PreparedStatement countStatement = connection.prepareStatement(countQuery);
								try {
									countStatement.setObject(1, key);
									countTrace.start();
									runningTraces.add(countTrace);
									ResultSet executeQuery = countStatement.executeQuery();
									if (!executeQuery.next()) {
										throw new ServiceException("JDBC-15", "Can not determine amount of elements in '" + tableName + "' for field " + aggregateKeyName + " = " + key);
									}
									long number = executeQuery.getLong(1);
									if (counts.get(key) + number > limit) {
										throw new ServiceException("JDBC-22", "Too many elements for table '" + tableName + "' field '" + aggregateKeyName + "' value '" + key + "', currently " + number + " in database and " + counts.get(key) + " would be added (> " + limit + ")");
									}
									countTrace.setRowCount(1);
									countTrace.stop();
									runningTraces.remove(countTrace);
								}
								finally {
									countStatement.close();
								}
							}
						}
					}
					if (changeTracker != null && trackChanges) {
						if (primaryKey == null) {
							for (Element<?> element : TypeUtils.getAllChildren(definition.getParameters())) {
								Value<Boolean> property = element.getProperty(PrimaryKeyProperty.getInstance());
								if (property != null && property.getValue()) {
									// if the input is generated and we set a collection name on the primary key, we assume it is for the table
									// for generated inputs, we can't set properties on the root
									if (definition.isInputGenerated()) {
										Value<String> collectionProperty = element.getProperty(CollectionNameProperty.getInstance());
										if (collectionProperty != null) {
											tableName = uncamelify(collectionProperty.getValue());
										}
									}
									primaryKey = element;
									break;
								}
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
						primaryKeyTypes = new ArrayList<Element<?>>();
						for (ComplexContent parameter : parameters) {
							Object key = parameter.get(primaryKey.getName());
							if (key == null) {
								throw new ServiceException("JDBC-23", "No primary key present in the input");
							}
							primaryKeys.add(key);
							primaryKeyTypes.add(parameter.getType().get(primaryKey.getName()));
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
						if (!preparedSql.trim().toLowerCase().startsWith("insert") || (originalSql.trim().toLowerCase().contains("on conflict") && originalSql.trim().toLowerCase().contains("do update"))) {
							// a select to see what actually changed (in change tracking)
							DatabaseRequestTrace selectTrace = tracer.newTrace(definition.getId(), "change-track-select-before", selectBuilder.toString());
							PreparedStatement selectAll = connection.prepareStatement(selectBuilder.toString());
							try {
								for (int i = 0; i < primaryKeys.size(); i++) {
//									selectAll.setObject(i + 1, primaryKeys.get(i));
									dialect.setObject(selectAll, primaryKeyTypes.get(i), i + 1, primaryKeys.get(i), preparedSql);
								}
								selectTrace.start();
								runningTraces.add(selectTrace);
								ResultSet executeQuery = selectAll.executeQuery();
								original = new HashMap<Object, Map<String, Object>>();
								ResultSetMetaData metaData = executeQuery.getMetaData();
								int columnCount = metaData.getColumnCount();
								while (executeQuery.next()) {
									Map<String, Object> current = new HashMap<String, Object>();
									for (int i = 1; i <= columnCount; i++) {
										// the column name can be uppercased
										current.put(metaData.getColumnLabel(i).toLowerCase(), executeQuery.getObject(i));
									}
									original.put(current.get(primaryKeyName), current);
								}
								missing.removeAll(original.keySet());
								selectTrace.setRowCount(original.size());
								selectTrace.stop();
								runningTraces.remove(selectTrace);
							}
							finally {
								selectAll.close();
							}
						}
					}
					
					DatabaseRequestTrace queryTrace = tracer.newTrace(definition.getId(), "query", preparedSql);
					MetricTimer timer = metrics == null ? null : metrics.start(METRIC_EXECUTION_TIME);
					queryTrace.start();
					runningTraces.add(queryTrace);
					int[] executeBatch = batchInputAdded ? statement.executeBatch() : new int[] { statement.executeUpdate() };
					int total = 0;
					for (int amount : executeBatch) {
						total += amount;
					}
					output.set(JDBCService.ROW_COUNT, total);
					queryTrace.setRowCount(total);
					queryTrace.stop();
					runningTraces.remove(queryTrace);
					Long executionTime = timer == null ? null : timer.stop();
					if (debug != null) {
						debug.setExecutionDuration(executionTime);
						debug.setOutputAmount(total);
					}
					
					if (changeTracker != null && trackChanges) {
						DatabaseRequestTrace selectTrace = tracer.newTrace(definition.getId(), "change-track-select-after", selectBuilder.toString());
						PreparedStatement selectAll = connection.prepareStatement(selectBuilder.toString());
						try {
							for (int i = 0; i < primaryKeys.size(); i++) {
//								selectAll.setObject(i + 1, primaryKeys.get(i));
								dialect.setObject(selectAll, primaryKeyTypes.get(i), i + 1, primaryKeys.get(i), preparedSql);
							}
							selectTrace.start();
							runningTraces.add(selectTrace);
							ResultSet executeQuery = selectAll.executeQuery();
							Map<Object, Map<String, Object>> updated = new HashMap<Object, Map<String, Object>>();
							ResultSetMetaData metaData = executeQuery.getMetaData();
							int columnCount = metaData.getColumnCount();
							while (executeQuery.next()) {
								Map<String, Object> current = new HashMap<String, Object>();
								for (int i = 1; i <= columnCount; i++) {
									current.put(metaData.getColumnLabel(i).toLowerCase(), executeQuery.getObject(i));
								}
								updated.put(current.get(primaryKeyName), current);
							}
							selectTrace.setRowCount(updated.size());
							selectTrace.stop();
							runningTraces.remove(selectTrace);
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
								Map<String, Object> old = original == null ? null : original.get(key);
								if (old == null) {
									old = new HashMap<String, Object>();
								}
								if (original != null) {
									for (String name : old.keySet()) {
										Object currentValue = current.get(name);
										Object oldValue = old.get(name);
										// if it is unchanged, remove it from the new mapping
										if (currentValue == null && oldValue == null || currentValue != null && currentValue.equals(oldValue)) {
											current.remove(name);
										}
									}
								}
								// if something was updated, add it to the diff
								if (!current.isEmpty()) {
									changesets.add(new ChangeSetImpl(key, ChangeType.UPDATE, old, current));
								}
							}
							if (!changesets.isEmpty()) {
								changeTracker.track(connectionId, transactionId, tableName, changesets);
							}
						}
						finally {
							selectAll.close();
						}
					}
				}
				else {
					ComplexType resultType = (ComplexType) getDefinition().getOutput().get(JDBCService.RESULTS).getType();
					MetricTimer timer = metrics == null ? null : metrics.start(METRIC_EXECUTION_TIME);
					// if we have an offset/limit but it was not fixed natively through the dialect, do it programmatically
					if (!nativeLimit && limit != null) {
						statement.setMaxRows((int) (offset != null ? offset + limit : limit));
					}
					DatabaseRequestTrace selectTrace = tracer.newTrace(definition.getId(), "query", preparedSql);
					selectTrace.start();
					runningTraces.add(selectTrace);
					if (offset != null) {
						selectTrace.setOffset(offset);
					}
					if (limit != null) {
						selectTrace.setLimit(limit);
					}
					ResultSet executeQuery = statement.executeQuery();
					Long executionTime = timer == null ? null : timer.stop();
					if (debug != null) {
						debug.setExecutionDuration(executionTime);
					}
					if (lazy) {
						if (language != null) {
							throw new ServiceException("JDBC-19", "Language is not supported in combination with lazy because of performance reasons");
						}
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
							boolean hasNext = false;
							while (executeQuery.next()) {
								// if we don't have a native (dialect) limit but we did set an offset, do it programmatically
								if (!nativeLimit && offset != null) {
									recordCounter++;
									if (recordCounter < offset) {
										continue;
									}
								}
								// index is 0-based and limit is 1-based
								// we upped the limit by 1 additional one to overselect
								if (overSelected && index > limit - 2) {
									hasNext = true;
									continue;
								}
								else if (hasNext) {
									throw new SQLException("We selected too many records, hasNext is already set, where are the additional records coming from?");
								}
								ComplexContent result;
								try {
									result = ResultSetCollectionHandler.convert(executeQuery, resultType);
								}
								catch (Exception e) {
									throw new ServiceException("JDBC-24", "Invalid type", e);
								}
								output.set(JDBCService.RESULTS + "[" + index++ + "]", result);
								output.set(JDBCService.HAS_NEXT, hasNext);
							}
							// if we have a language that differs from the default one, we need to translate the results
							if (language != null && dataSourceProvider instanceof DataSourceWithTranslator && ((DataSourceWithTranslator) dataSourceProvider).getTranslator() != null
								&& (((DataSourceWithTranslator) dataSourceProvider).getDefaultLanguage() == null || !((DataSourceWithTranslator) dataSourceProvider).getDefaultLanguage().equals(language))) {
								postTranslate(
									connectionId,
									transactionId,
									language,
									((DataSourceWithTranslator) dataSourceProvider).getTranslator(),
									(List<ComplexContent>) output.get(JDBCService.RESULTS),
									statement);
							}
						}
						finally {
							executeQuery.close();
						}
						selectTrace.setRowCount(index);
						selectTrace.stop();
						runningTraces.remove(selectTrace);
						output.set(JDBCService.ROW_COUNT, index);
						Long mappingTime = timer == null ? null : timer.stop();
						if (debug != null) {
							debug.setMappingDuration(mappingTime);
							debug.setOutputAmount(index);
						}
					}
				}
				if (totalCountStatement != null) {
					totalCountTrace.start();
					runningTraces.add(totalCountTrace);
					MetricTimer countTimer = metrics == null ? null : metrics.start(METRIC_COUNT_TIME);
					ResultSet totalCountResultSet = totalCountStatement.executeQuery();
					if (totalCountResultSet.next()) {
						output.set(JDBCService.TOTAL_ROW_COUNT, totalCountResultSet.getLong(1));
					}
					else {
						// apparently when doing a wrapping count over a grouped query, it can return null results if no results are found in the inner select
						// so we set to 0 instead of throwing an exception (the old behavior)
						output.set(JDBCService.TOTAL_ROW_COUNT, 0);
					}
					Long countTime = countTimer == null ? null : countTimer.stop();
					if (debug != null) {
						debug.setTotalCountDuration(countTime);
					}
					totalCountTrace.setRowCount(1);
					totalCountTrace.stop();
					runningTraces.remove(totalCountTrace);
				}
				else if (includeTotalCount) {
					output.set(JDBCService.TOTAL_ROW_COUNT, output.get(JDBCService.ROW_COUNT));
				}
				
				if (generatedColumn != null) {
					ResultSet generatedKeys = statement.getGeneratedKeys();
					try {
						List<Long> generated = new ArrayList<Long>();
						while (generatedKeys.next()) {
							generated.add(generatedKeys.getLong(1));
						}
						output.set(JDBCService.GENERATED_KEYS, generated);
					}
					finally {
						generatedKeys.close();
					}
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
				if (totalCountStatement != null) {
					totalCountStatement.close();
				}
			}
		}
		catch (SQLException e) {
			failTraces(runningTraces, e);
			while (e.getNextException() != null) {
				e = e.getNextException();
			}
			logger.warn("Failed jdbc service " + definition.getId() + ", original sql: " + originalSql + ",\nfinal sql: " + preparedSql, e);
			throw new ServiceException(e);
		}
		catch (Exception e) {
			failTraces(runningTraces, e);
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
			if (tracer != null) {
				try {
					tracer.close();
				}
				catch (Exception e) {
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
	
	private void failTraces(List<Trace> traces, Exception e) {
		for (Trace trace : traces) {
			try {
				trace.error(null, e);
			}
			catch (Exception f) {
				logger.warn("Could not stop tracer", f);
			}
		}
	}

	private void postTranslate(String connectionId, String transactionId, String language, JDBCTranslator translator, List<ComplexContent> list, PreparedStatement statement) throws ServiceException, SQLException {
		if (list != null && !list.isEmpty()) {
			Element<?> primaryKey = null;
			String tableName = null;
			List<String> ids = new ArrayList<String>();
			for (ComplexContent parameter : list) {
				if (tableName == null) {
					String collectionProperty = ValueUtils.getValue(CollectionNameProperty.getInstance(), parameter.getType().getProperties());
					if (collectionProperty != null) {
						tableName = uncamelify(collectionProperty);
					}
				}
				if (primaryKey == null) {
					for (Element<?> child : TypeUtils.getAllChildren(parameter.getType())) {
						Value<Boolean> primaryKeyProperty = child.getProperty(PrimaryKeyProperty.getInstance());
						if (primaryKeyProperty != null && primaryKeyProperty.getValue()) {
							if (tableName == null) {
								// if the input is generated and we set a collection name on the primary key, we assume it is for the table
								// for generated inputs, we can't set properties on the root
								Value<String> collectionProperty = child.getProperty(CollectionNameProperty.getInstance());
								if (collectionProperty != null) {
									tableName = uncamelify(collectionProperty.getValue());
								}
							}
							primaryKey = child;
						}
						break;
					}
				}
				
				if (primaryKey == null) {
					throw new ServiceException("JDBC-18", "Primary key needed to perform auto-translations");
				}
				Object idObject = parameter.get(primaryKey.getName());
				if (idObject == null) {
					throw new ServiceException("JDBC-11", "No primary key present in the input");
				}
				
				String id = null;
				// if the primary key is a uuid, it is globally unique, no additional identifier necessary
				if (UUID.class.isAssignableFrom(((SimpleType<?>) primaryKey.getType()).getInstanceClass())) {
					id = idObject.toString().replace("-", "");
				}
				// a stringified uuid
				else if (idObject instanceof String && idObject.toString().matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}|[0-9a-fA-F]{32}")) {
					id = idObject.toString().replace("-", "");
				}
				else if (tableName == null) {
					tableName = statement.getMetaData().getTableName(1);
					if (tableName == null) {
						throw new ServiceException("JDBC-14", "Can not determine the table name for translations");
					}
				}
				else {
					id = tableName + ":" + converter.convert(idObject, String.class);
				}
				ids.add(id);
			}
			if (!ids.isEmpty()) {
				List<Translation> translations = translator.get(connectionId, transactionId, language, ids);
				if (translations != null && !translations.isEmpty()) {
					// hash the translations
					Map<String, List<Translation>> hashmap = new HashMap<String, List<Translation>>();
					for (Translation translation : translations) {
						if (!hashmap.containsKey(translation.getId())) {
							hashmap.put(translation.getId(), new ArrayList<Translation>());
						}
						hashmap.get(translation.getId()).add(translation);
					}
					for (ComplexContent parameter : list) {
						Object idObject = parameter.get(primaryKey.getName());
						String id = null;
						// if the primary key is a uuid, it is globally unique, no additional identifier necessary
						if (((SimpleType<?>) primaryKey.getType()).getInstanceClass().equals(UUID.class)) {
							id = idObject.toString().replace("-", "");
						}
						else if (idObject instanceof String && idObject.toString().matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}|[0-9a-fA-F]{32}")) {
							id = idObject.toString().replace("-", "");
						}
						else {
							id = tableName + ":" + converter.convert(idObject, String.class);
						}
						List<Translation> contentTranslations = hashmap.get(id);
						if (contentTranslations != null && !contentTranslations.isEmpty()) {
							for (Translation translation : contentTranslations) {
								parameter.set(translation.getName(), translation.getTranslation());
							}
						}
					}
				}
			}
		}
	}

	public static String replaceAffixes(JDBCService definition, List<AffixMapping> affixes, String preparedSql) {
		if (affixes != null) {
			String match = null;
			// we search the most specific match but matches that are equally specific are all used as you may be interested in giving different affixes to different tables
			List<AffixMapping> applicableAffixes = new ArrayList<AffixMapping>();
			for (AffixMapping mapping : affixes) {
				if (mapping.getContext() == null || definition.getId().equals(mapping.getContext()) || definition.getId().startsWith(mapping.getContext() + ".")) {
					// if we already have a match, check that the new match is more precise, otherwise we skip it
					if (match != null) {
						if (mapping.getContext() == null || mapping.getContext().length() < match.length()) {
							continue;
						}
					}
					match = mapping.getContext();
					applicableAffixes.add(mapping);
				}
			}
			// we sort the affixes with specific tables to the front as they are least likely to overlap
			// generic catch all affixes (with no tables) are pushed to the back
			Collections.sort(applicableAffixes, new Comparator<AffixMapping>() {
				@Override
				public int compare(AffixMapping o1, AffixMapping o2) {
					boolean o1HasTables = o1.getTables() != null && !o1.getTables().isEmpty();
					boolean o2HasTables = o2.getTables() != null && !o2.getTables().isEmpty();
					if (o1HasTables && !o2HasTables) {
						return -1;
					}
					else if (o2HasTables && !o1HasTables) {
						return 1;
					}
					else {
						return 0;
					}
				}
			});
			for (AffixMapping mapping : applicableAffixes) {
				String affix = mapping.getAffix() == null ? "" : mapping.getAffix();
				if (mapping.getTables() != null && !mapping.getTables().isEmpty()) {
					for (String table : mapping.getTables()) {
						// prefix
						preparedSql = preparedSql.replaceAll("~" + table + "\\b", affix + table);
						// suffix
						preparedSql = preparedSql.replaceAll("\\b" + table + "~", table + affix);
					}
				}
				// do a replace all for remaining tildes based on the given affix
				else {
					preparedSql = preparedSql.replace("~", affix);
				}
			}
		}
		
		// replace any remaining affix notations (should only be because you didn't have any...)
		preparedSql = preparedSql.replace("~", "");
		return preparedSql;
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
