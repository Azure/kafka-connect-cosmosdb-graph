/**
 * 
 */
package com.microsoft.cosmos.gremlin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

/**
 * Entry point into Gremlin sink connector
 * 
 * @author olignat
 *
 */
public final class KafkaGremlinSinkConnector extends SinkConnector {

	public enum Keys {
		;
		static final String HOST = "host";
		static final String PORT = "port";
		static final String DATABASE = "database";
		static final String CONTAINER = "container";
		static final String KEY = "key";
		static final String TRAVERSAL = "traversal";
		static final String ENABLE_SKIP_ON_CONFLICT = "enableSkipOnConflict";
		static final String ENABLE_SSL = "enableSsl";
		static final String ENABLE_ERROR_ON_EMPTY_RESULTS = "enableErrorOnEmptyResult";
		static final String MAX_WAIT_FOR_CONNECTION_MILLISECONDS = "maxWaitForConnectionMilliseconds";
		static final String RECORD_WRITE_RETRY_COUNT = "recordWriteRetryCount";
		static final String RECORD_WRITE_RETRY_MILLISECONDS = "recordWriteRetryMilliseconds";
	}

	static final int DEFAULT_PORT = 443;
	static final boolean DEFAULT_ENABLE_SKIP_ON_CONFLICT = false;
	static final boolean DEFAULT_ENABLE_SSL = true;
	static final boolean DEFAULT_ENABLE_ERROR_ON_EMPTY_RESULTS = false;
	static final int DEFAULT_MAX_WAIT_FOR_CONNECTION_MILLISECONDS = 15000;
	static final int DEFAULT_RECORD_WRITE_RETRY_COUNT = 3;
	static final int DEFAULT_RECORD_WRITE_RETRY_MILLISECONDS = 1000;

	private static final ConfigDef CONFIG_DEF = new ConfigDef().define(Keys.HOST, Type.STRING, "", Importance.HIGH,
			"Microsoft Azure Cosmos Gremlin Accounty fully qualified name in the format *.gremlin.cosmos.azure.com")
			.define(Keys.PORT, Type.INT, KafkaGremlinSinkConnector.DEFAULT_PORT, Importance.HIGH,
					"Port number to which to send traffic at the host")
			.define(Keys.DATABASE, Type.STRING, "", Importance.HIGH, "Database inside global database account")
			.define(Keys.CONTAINER, Type.STRING, "", Importance.HIGH, "Container or collection inside database")
			.define(Keys.KEY, Type.STRING, "", Importance.HIGH, "Primary or secondary authentication key")
			.define(Keys.TRAVERSAL, Type.STRING, "", Importance.HIGH,
					"Gremlin query to execute for every event. Use ${key.property} or ${value.property} marker to match fields in MAP and STRUCT messages. For primitive types use simple ${key} and ${value} markers instead. For arrays it is possible to match an entire array with ${key} or ${value} or specific zero-based position in an array with ${key[5]} or ${value[0]}.")
			.define(Keys.ENABLE_SKIP_ON_CONFLICT, Type.BOOLEAN,
					KafkaGremlinSinkConnector.DEFAULT_ENABLE_SKIP_ON_CONFLICT, Importance.MEDIUM,
					"When enabled connector will skip over traversals that result in conflicting writes and just drop the records rather than fail and stall the flow of messages.")
			.define(Keys.ENABLE_SSL, Type.BOOLEAN, KafkaGremlinSinkConnector.DEFAULT_ENABLE_SSL, Importance.MEDIUM,
					"Flag that controls whether SSL is enabled or disabled. SSL is required for Microsoft Azure Cosmos DB accounts but can be disabled for local testing with emulator.")
			.define(Keys.ENABLE_ERROR_ON_EMPTY_RESULTS, Type.BOOLEAN,
					KafkaGremlinSinkConnector.DEFAULT_ENABLE_ERROR_ON_EMPTY_RESULTS, Importance.MEDIUM,
					"Flag that turns empty result from gremlin traversal into a connector error.")
			.define(Keys.MAX_WAIT_FOR_CONNECTION_MILLISECONDS, Type.INT,
					KafkaGremlinSinkConnector.DEFAULT_MAX_WAIT_FOR_CONNECTION_MILLISECONDS, Importance.MEDIUM,
					"Amount of time a client would wait for a connection to Microsoft Azure Cosmos DB before giving up.")
			.define(Keys.RECORD_WRITE_RETRY_COUNT, Type.INT, KafkaGremlinSinkConnector.DEFAULT_RECORD_WRITE_RETRY_COUNT,
					Importance.MEDIUM,
					"Number of times to attempt to write a record to Microsoft Azure Cosmos DB account before giving up.")
			.define(Keys.RECORD_WRITE_RETRY_MILLISECONDS, Type.INT,
					KafkaGremlinSinkConnector.DEFAULT_RECORD_WRITE_RETRY_MILLISECONDS, Importance.MEDIUM,
					"Default retry interval for a failed attempt to write a record into Microsoft Azure Cosmos DB account.");

	private String host;
	private String port;
	private String database;
	private String container;
	private String key;
	private String traversal;
	private String enableSkipOnConflict;
	private String enableSsl;
	private String enableErrorOnEmptyResult;
	private String maxWaitForConnectionMilliseconds;
	private String recordWriteRetryCount;
	private String recordWriteRetryMilliseconds;

	public String version() {
		return AppInfoParser.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		this.host = props.get(Keys.HOST);
		if (this.host == null || this.host.isEmpty()) {
			throw new ConfigException(Keys.HOST, "<empty>",
					"Global database account address is required to establish connection");
		}

		this.port = props.get(Keys.PORT);
		if (this.port == null || this.port.isEmpty()) {
			throw new ConfigException(Keys.PORT, "<empty>", "Port is required to establish connection");
		}

		this.database = props.get(Keys.DATABASE);
		if (this.database == null || this.database.isEmpty()) {
			throw new ConfigException(Keys.DATABASE, "<empty>", "Database name is required to establish connection");
		}

		this.container = props.get(Keys.CONTAINER);
		if (this.container == null || this.container.isEmpty()) {
			throw new ConfigException(Keys.CONTAINER, "<empty>", "Container name is required to establish connection");
		}

		this.key = props.get(Keys.KEY);
		if (this.key == null || this.key.isEmpty()) {
			throw new ConfigException(Keys.KEY, "<empty>", "Authentication key is required to establish connection");
		}

		this.traversal = props.get(Keys.TRAVERSAL);
		this.enableSkipOnConflict = props.get(Keys.ENABLE_SKIP_ON_CONFLICT);
		this.enableSsl = props.get(Keys.ENABLE_SSL);
		this.enableErrorOnEmptyResult = props.get(Keys.ENABLE_ERROR_ON_EMPTY_RESULTS);
		this.maxWaitForConnectionMilliseconds = props.get(Keys.MAX_WAIT_FOR_CONNECTION_MILLISECONDS);
		this.recordWriteRetryCount = props.get(Keys.RECORD_WRITE_RETRY_COUNT);
		this.recordWriteRetryMilliseconds = props.get(Keys.RECORD_WRITE_RETRY_MILLISECONDS);
	}

	@Override
	public Class<? extends Task> taskClass() {
		return KafkaGremlinSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		ArrayList<Map<String, String>> configs = new ArrayList<Map<String, String>>();
		for (int i = 0; i < maxTasks; i++) {
			Map<String, String> config = new HashMap<String, String>();

			config.put(Keys.HOST, this.host);
			config.put(Keys.PORT, this.port);
			config.put(Keys.DATABASE, this.database);
			config.put(Keys.CONTAINER, this.container);
			config.put(Keys.KEY, this.key);

			if (this.traversal != null && !this.traversal.isEmpty()) {
				config.put(Keys.TRAVERSAL, this.traversal);
			}

			if (this.enableSkipOnConflict != null && !this.enableSkipOnConflict.isEmpty()) {
				config.put(Keys.ENABLE_SKIP_ON_CONFLICT, this.enableSkipOnConflict);
			}

			if (this.enableSsl != null && !this.enableSsl.isEmpty()) {
				config.put(Keys.ENABLE_SSL, this.enableSsl);
			}

			if (this.enableErrorOnEmptyResult != null && !this.enableErrorOnEmptyResult.isEmpty()) {
				config.put(Keys.ENABLE_ERROR_ON_EMPTY_RESULTS, this.enableErrorOnEmptyResult);
			}

			if (this.maxWaitForConnectionMilliseconds != null && !this.maxWaitForConnectionMilliseconds.isEmpty()) {
				config.put(Keys.MAX_WAIT_FOR_CONNECTION_MILLISECONDS, this.maxWaitForConnectionMilliseconds);
			}

			if (this.recordWriteRetryCount != null && !this.recordWriteRetryCount.isEmpty()) {
				config.put(Keys.RECORD_WRITE_RETRY_COUNT, this.recordWriteRetryCount);
			}

			if (this.recordWriteRetryMilliseconds != null && !this.recordWriteRetryMilliseconds.isEmpty()) {
				config.put(Keys.RECORD_WRITE_RETRY_MILLISECONDS, this.recordWriteRetryMilliseconds);
			}

			configs.add(config);
		}

		return configs;
	}

	@Override
	public void stop() {
	}

	@Override
	public ConfigDef config() {
		return CONFIG_DEF;
	}
}