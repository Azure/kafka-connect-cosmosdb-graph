/**
 * 
 */
package com.microsoft.cosmos.gremlin;

import java.time.Duration;
import java.time.LocalTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.tinkerpop.gremlin.driver.AuthProperties;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV1d0;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sink task used to replicate data from Kafka into Cosmos DB Gremlin account
 * 
 * @author olignat
 *
 */
public final class KafkaGremlinSinkTask extends SinkTask {

	private static final Logger log = LoggerFactory.getLogger(KafkaGremlinSinkTask.class);

	private static final String X_MS_STATUS_CODE_HEADER = "x-ms-status-code";
	private static final String X_MS_RETRY_AFTER_MS_HEADER = "x-ms-retry-after-ms";

	private static final int X_MS_STATUS_CODE_VALUE_UNKNOWN = -1;
	private static final int X_MS_STATUS_CODE_VALUE_CONFLICT = 409;
	private static final int X_MS_MAX_RETRY_AFTER_MS = 10000;

	private String host;
	private int port;
	private String database;
	private String container;
	private String key;
	private GremlinQueryBuilder.GremlinParameterizedQuery parameterizedTraversal;
	private Boolean enableSkipOnConflict;
	private Boolean enableSsl;
	private Boolean enableErrorOnEmptyResult;
	private int maxWaitForConnectionMilliseconds;
	private int recordWriteRetryCount;
	private int recordWriteRetryMilliseconds;

	private Cluster cluster;
	private Client client;

	private int remainingRetries;

	public KafkaGremlinSinkTask() {
	}

	public String version() {
		return new KafkaGremlinSinkConnector().version();
	}

	@Override
	public void start(Map<String, String> props) {
		this.host = props.get(KafkaGremlinSinkConnector.Keys.HOST);
		this.port = Integer.parseInt(props.get(KafkaGremlinSinkConnector.Keys.PORT));
		this.database = props.get(KafkaGremlinSinkConnector.Keys.DATABASE);
		this.container = props.get(KafkaGremlinSinkConnector.Keys.CONTAINER);
		this.key = props.get(KafkaGremlinSinkConnector.Keys.KEY);

		// Process traversal and prepare for execution
		this.parameterizedTraversal = GremlinQueryBuilder
				.parameterize(props.get(KafkaGremlinSinkConnector.Keys.TRAVERSAL));

		if (props.containsKey(KafkaGremlinSinkConnector.Keys.ENABLE_SKIP_ON_CONFLICT)) {
			this.enableSkipOnConflict = Boolean
					.parseBoolean(props.get(KafkaGremlinSinkConnector.Keys.ENABLE_SKIP_ON_CONFLICT));
		} else {
			this.enableSkipOnConflict = KafkaGremlinSinkConnector.DEFAULT_ENABLE_SKIP_ON_CONFLICT;
		}

		if (props.containsKey(KafkaGremlinSinkConnector.Keys.ENABLE_SSL)) {
			this.enableSsl = Boolean.parseBoolean(props.get(KafkaGremlinSinkConnector.Keys.ENABLE_SSL));
		} else {
			this.enableSsl = KafkaGremlinSinkConnector.DEFAULT_ENABLE_SSL;
		}

		if (props.containsKey(KafkaGremlinSinkConnector.Keys.ENABLE_ERROR_ON_EMPTY_RESULTS)) {
			this.enableErrorOnEmptyResult = Boolean
					.parseBoolean(props.get(KafkaGremlinSinkConnector.Keys.ENABLE_ERROR_ON_EMPTY_RESULTS));
		} else {
			this.enableErrorOnEmptyResult = KafkaGremlinSinkConnector.DEFAULT_ENABLE_ERROR_ON_EMPTY_RESULTS;
		}

		if (props.containsKey(KafkaGremlinSinkConnector.Keys.MAX_WAIT_FOR_CONNECTION_MILLISECONDS)) {
			this.maxWaitForConnectionMilliseconds = Integer
					.parseInt(props.get(KafkaGremlinSinkConnector.Keys.MAX_WAIT_FOR_CONNECTION_MILLISECONDS));
		} else {
			this.maxWaitForConnectionMilliseconds = KafkaGremlinSinkConnector.DEFAULT_MAX_WAIT_FOR_CONNECTION_MILLISECONDS;
		}

		if (props.containsKey(KafkaGremlinSinkConnector.Keys.RECORD_WRITE_RETRY_COUNT)) {
			this.recordWriteRetryCount = Integer
					.parseInt(props.get(KafkaGremlinSinkConnector.Keys.RECORD_WRITE_RETRY_COUNT));
		} else {
			this.recordWriteRetryCount = KafkaGremlinSinkConnector.DEFAULT_RECORD_WRITE_RETRY_COUNT;
		}

		if (props.containsKey(KafkaGremlinSinkConnector.Keys.RECORD_WRITE_RETRY_MILLISECONDS)) {
			this.recordWriteRetryMilliseconds = Integer
					.parseInt(props.get(KafkaGremlinSinkConnector.Keys.RECORD_WRITE_RETRY_MILLISECONDS));
		} else {
			this.recordWriteRetryMilliseconds = KafkaGremlinSinkConnector.DEFAULT_RECORD_WRITE_RETRY_MILLISECONDS;
		}

		// Stop in case we are started
		this.stop();

		Cluster.Builder builder = Cluster.build();
		builder.addContactPoint(this.host);
		builder.port(this.port);
		builder.maxWaitForConnection(this.maxWaitForConnectionMilliseconds);

		AuthProperties authenticationProperties = new AuthProperties();
		authenticationProperties.with(AuthProperties.Property.USERNAME,
				String.format("/dbs/%s/colls/%s", this.database, this.container));
		authenticationProperties.with(AuthProperties.Property.PASSWORD, this.key);

		builder.authProperties(authenticationProperties);
		builder.enableSsl(this.enableSsl);

		Map<String, Object> config = new HashMap<String, Object>();
		config.put("serializeResultToString", "true");

		GraphSONMessageSerializerV1d0 serializer = new GraphSONMessageSerializerV1d0();
		serializer.configure(config, null);

		builder.serializer(serializer);

		// Configure special load balancing strategy for Azure that ignores host
		// unavailability
		// and continues to talk to the same host
		builder.loadBalancingStrategy(new StickyLoadBalancingStrategy());

		this.cluster = builder.create();
		this.client = this.cluster.connect();

		this.remainingRetries = this.recordWriteRetryCount;
	}

	public void put(Collection<SinkRecord> sinkRecords) {
		for (SinkRecord sinkRecord : sinkRecords) {
			// Materialize traversal parameters
			Map<String, Object> materializedTraversalParameters = GremlinQueryBuilder
					.materialize(this.parameterizedTraversal, sinkRecord);
			log.debug("Executing {} with {} parameters", this.parameterizedTraversal.getParameterizedTraversal(),
					materializedTraversalParameters.size());

			try {
				ResultSet resultSet = this.client.submit(this.parameterizedTraversal.getParameterizedTraversal(),
						materializedTraversalParameters);
				List<Result> results = resultSet.all().get();

				if (results == null || results.isEmpty()) {
					log.debug("Completed successfully without results");

					if (this.enableErrorOnEmptyResult) {
						throw new ConnectException("Completed successfully without results");
					}
				} else {
					for (Result result : results) {
						log.debug("Result {}", result.toString());
					}
				}
			} catch (Exception e) {
				log.error("Write failed {}, remaining retries = {}", e.toString(), this.remainingRetries);

				int targetRecordWriteRetryMilliseconds = this.recordWriteRetryMilliseconds;

				// Special case for known errors when conflicting documents are being inserted
				ResponseException re = KafkaGremlinSinkTask.getResponseExceptionIfPossible(e);
				if (re != null) {
					// Check for known errors that need to be retried or skipped
					if (re.getStatusAttributes().isPresent()) {
						Map<String, Object> attributes = re.getStatusAttributes().get();
						int statusCode = (int) attributes.getOrDefault(KafkaGremlinSinkTask.X_MS_STATUS_CODE_HEADER,
								KafkaGremlinSinkTask.X_MS_STATUS_CODE_VALUE_UNKNOWN);

						// Now we can check for specific conditions
						if (statusCode == KafkaGremlinSinkTask.X_MS_STATUS_CODE_VALUE_CONFLICT) {
							if (this.enableSkipOnConflict) {
								// Do not retry on this error - move on to next item
								log.warn(
										"Record in partition {} and offset {} resulted in conflicting traversal. Record is skipped.",
										sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset());
								continue;
							}
						}

						// Check if we need to delay retry
						if (attributes.containsKey(KafkaGremlinSinkTask.X_MS_RETRY_AFTER_MS_HEADER)) {
							int suggestedRetryAfter = KafkaGremlinSinkTask.parseTimeSpan(
									(String) attributes.get(KafkaGremlinSinkTask.X_MS_RETRY_AFTER_MS_HEADER));
							if (suggestedRetryAfter > 0) {
								// Use suggestion within reasonable bounds
								targetRecordWriteRetryMilliseconds = Math.min(suggestedRetryAfter,
										KafkaGremlinSinkTask.X_MS_MAX_RETRY_AFTER_MS);
							}
						}
					}
				}

				if (this.remainingRetries == 0) {
					throw new ConnectException(e);
				}

				this.remainingRetries -= 1;
				this.context.timeout(targetRecordWriteRetryMilliseconds);
				throw new RetriableException(e);
			}
		}

		this.remainingRetries = this.recordWriteRetryCount;
	}

	public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
	}

	@Override
	public void stop() {
		if (this.client != null) {
			this.client.close();
			this.client = null;
		}

		if (this.cluster != null) {
			this.cluster.close();
			this.cluster = null;
		}
	}

	private static ResponseException getResponseExceptionIfPossible(Throwable e) {
		if (e == null) {
			return null;
		}

		if (e instanceof ResponseException) {
			return (ResponseException) e;
		}

		if (e.getCause() != null) {
			return getResponseExceptionIfPossible(e.getCause());
		}

		return null;
	}

	/**
	 * Parse a string in format "00:00:00.5000000" and return total milliseconds it
	 * represents.
	 * 
	 * @param timeSpanString is the string to parse
	 * @return a value parsed out of the string or -1 if parsing failed
	 * 
	 * @author olignat
	 */
	private static int parseTimeSpan(String timeSpanString) {
		try {
			// Sanity check, in case we got a simple integer number
			return Integer.parseInt(timeSpanString);
		} catch (NumberFormatException e) {
			// Do nothing and keep parsing
		}

		try {
			LocalTime locaTime = LocalTime.parse(timeSpanString);
			Duration duration = Duration.between(LocalTime.MIN, locaTime);
			return (int) duration.toMillis();
		} catch (Exception e) {
			// We couldn't parse
		}

		// We couldn't parse it
		return -1;
	}
}