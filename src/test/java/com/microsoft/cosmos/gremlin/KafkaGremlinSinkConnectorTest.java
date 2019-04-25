/**
 * 
 */
package com.microsoft.cosmos.gremlin;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import com.microsoft.cosmos.gremlin.KafkaGremlinSinkConnector.Keys;

/**
 * Test coverage for sink connector
 * 
 * @author olignat
 *
 */
final public class KafkaGremlinSinkConnectorTest {

	@Test
	public void testConfigurationDefinition() throws Exception {
		KafkaGremlinSinkConnector connector = new KafkaGremlinSinkConnector();
		ConfigDef configurationDefinition = connector.config();

		assertNotNull(configurationDefinition, "Configuration definition should not be null");
		assertFalse(configurationDefinition.names().isEmpty(), "Set of configuration names should not be empty");
		
		for (String configurationKey : configurationDefinition.names()) {
			ConfigKey configurationKeyDefinition = configurationDefinition.configKeys().get(configurationKey);
			
			assertNotNull(configurationKeyDefinition.name, "Configuration key definition name should not be null");
			assertFalse(configurationKeyDefinition.name.isEmpty(), "Configuration key definition name should not be empty");

			assertNotNull(configurationKeyDefinition.documentation, "Configuration key " + configurationKeyDefinition.name + " documentation name should not be null");
			assertFalse(configurationKeyDefinition.documentation.isEmpty(), "Configuration key " + configurationKeyDefinition.name + " documentation name should not be empty");
		}
		
		// Ensure it contains all expected keys
		assertTrue(configurationDefinition.configKeys().containsKey(Keys.HOST), "Key " + Keys.HOST + " should be present in configuration definition");
		assertTrue(configurationDefinition.configKeys().containsKey(Keys.PORT), "Key " + Keys.PORT + " should be present in configuration definition");
		assertTrue(configurationDefinition.configKeys().containsKey(Keys.DATABASE), "Key " + Keys.DATABASE + " should be present in configuration definition");
		assertTrue(configurationDefinition.configKeys().containsKey(Keys.CONTAINER), "Key " + Keys.CONTAINER + " should be present in configuration definition");
		assertTrue(configurationDefinition.configKeys().containsKey(Keys.KEY), "Key " + Keys.KEY + " should be present in configuration definition");
		assertTrue(configurationDefinition.configKeys().containsKey(Keys.TRAVERSAL), "Key " + Keys.TRAVERSAL + " should be present in configuration definition");
		assertTrue(configurationDefinition.configKeys().containsKey(Keys.ENABLE_SKIP_ON_CONFLICT), "Key " + Keys.ENABLE_SKIP_ON_CONFLICT + " should be present in configuration definition");
		assertTrue(configurationDefinition.configKeys().containsKey(Keys.ENABLE_SSL), "Key " + Keys.ENABLE_SSL + " should be present in configuration definition");
		assertTrue(configurationDefinition.configKeys().containsKey(Keys.MAX_WAIT_FOR_CONNECTION_MILLISECONDS), "Key " + Keys.MAX_WAIT_FOR_CONNECTION_MILLISECONDS + " should be present in configuration definition");
		assertTrue(configurationDefinition.configKeys().containsKey(Keys.RECORD_WRITE_RETRY_COUNT), "Key " + Keys.RECORD_WRITE_RETRY_COUNT + " should be present in configuration definition");
		assertTrue(configurationDefinition.configKeys().containsKey(Keys.RECORD_WRITE_RETRY_MILLISECONDS), "Key " + Keys.RECORD_WRITE_RETRY_MILLISECONDS + " should be present in configuration definition");
		
		assertEquals(11, configurationDefinition.configKeys().keySet().size(), "Unexpected configuration values found in configuration definition");
	}
	
	@Test
	public void testVersion() throws Exception {
		KafkaGremlinSinkConnector connector = new KafkaGremlinSinkConnector();
		assertEquals(connector.version(), "2.2.0", "Expected specific version to be returned by connector");
	}
	
	@Test
	public void testSinkTaskClass() throws Exception {
		KafkaGremlinSinkConnector connector = new KafkaGremlinSinkConnector();
		assertEquals(connector.taskClass(), KafkaGremlinSinkTask.class, "Expected sink task to be returned by connector");
	}
	
	@Test
	public void testStartEmptyConfiguration() throws Exception {
		KafkaGremlinSinkConnector connector = new KafkaGremlinSinkConnector();
		
		Map<String, String> configuration = new HashMap<String, String>();
		assertThrows(ConfigException.class, () -> connector.start(configuration)); 
	}
	
	@Test
	public void testStartRequiredConfiguration() throws Exception {
		KafkaGremlinSinkConnector connector = new KafkaGremlinSinkConnector();
		
		Map<String, String> configuration = new HashMap<String, String>();

		configuration.put(Keys.HOST, "");
		assertThrows(ConfigException.class, () -> connector.start(configuration));
		
		configuration.put(Keys.HOST, "test value");
		assertThrows(ConfigException.class, () -> connector.start(configuration));

		configuration.put(Keys.PORT, "");
		assertThrows(ConfigException.class, () -> connector.start(configuration));
		
		configuration.put(Keys.PORT, "321");
		assertThrows(ConfigException.class, () -> connector.start(configuration));
		
		configuration.put(Keys.DATABASE, "");
		assertThrows(ConfigException.class, () -> connector.start(configuration));

		configuration.put(Keys.DATABASE, "database");
		assertThrows(ConfigException.class, () -> connector.start(configuration));

		configuration.put(Keys.CONTAINER, "");
		assertThrows(ConfigException.class, () -> connector.start(configuration));
		
		configuration.put(Keys.CONTAINER, "container");
		assertThrows(ConfigException.class, () -> connector.start(configuration));

		configuration.put(Keys.KEY, "");
		assertThrows(ConfigException.class, () -> connector.start(configuration));
		
		configuration.put(Keys.KEY, "key");
		connector.start(configuration);

		configuration.put(Keys.TRAVERSAL, "");
		connector.start(configuration);
		
		configuration.put(Keys.TRAVERSAL, "traversal");
		connector.start(configuration);

		configuration.put(Keys.ENABLE_SKIP_ON_CONFLICT, "");
		connector.start(configuration);
		
		configuration.put(Keys.ENABLE_SKIP_ON_CONFLICT, "true");
		connector.start(configuration);

		configuration.put(Keys.ENABLE_SSL, "");
		connector.start(configuration);
		
		configuration.put(Keys.ENABLE_SSL, "false");
		connector.start(configuration);
		
		configuration.put(Keys.MAX_WAIT_FOR_CONNECTION_MILLISECONDS, "");
		connector.start(configuration);
		
		configuration.put(Keys.MAX_WAIT_FOR_CONNECTION_MILLISECONDS, "12345");
		connector.start(configuration);

		configuration.put(Keys.RECORD_WRITE_RETRY_COUNT, "");
		connector.start(configuration);
		
		configuration.put(Keys.RECORD_WRITE_RETRY_COUNT, "54321");
		connector.start(configuration);

		configuration.put(Keys.RECORD_WRITE_RETRY_MILLISECONDS, "");
		connector.start(configuration);
		
		configuration.put(Keys.RECORD_WRITE_RETRY_MILLISECONDS, "3");
		connector.start(configuration);
	}
	
	@Test
	public void testTaskConfigs() throws Exception {
		KafkaGremlinSinkConnector connector = new KafkaGremlinSinkConnector();
		
		Map<String, String> configuration = new HashMap<String, String>();

		configuration.put(Keys.HOST, "host" + UUID.randomUUID().toString());
		configuration.put(Keys.PORT, "port-" + UUID.randomUUID().toString());
		configuration.put(Keys.DATABASE, "database-" + UUID.randomUUID().toString());
		configuration.put(Keys.CONTAINER, "container-" + UUID.randomUUID().toString());
		configuration.put(Keys.KEY, "key-" + UUID.randomUUID().toString());
		configuration.put(Keys.TRAVERSAL, "traversal");
		configuration.put(Keys.ENABLE_SKIP_ON_CONFLICT, "true");
		configuration.put(Keys.ENABLE_SSL, "true");
		configuration.put(Keys.MAX_WAIT_FOR_CONNECTION_MILLISECONDS, "10001");
		configuration.put(Keys.RECORD_WRITE_RETRY_COUNT, "7");
		configuration.put(Keys.RECORD_WRITE_RETRY_MILLISECONDS, "1324");
		connector.start(configuration);
		
		List<Map<String, String>> taskConfigs = connector.taskConfigs(2);
		
		assertEquals(2, taskConfigs.size(), "Expected exactly two configurations because only two task configurations were requested");
		
		for (Map<String, String> taskConfig : taskConfigs) {
			for (String taskConfigKey : configuration.keySet()) {
				assertEquals(configuration.get(taskConfigKey), taskConfig.get(taskConfigKey), "Expected task configuration for " + taskConfigKey + " to match connector");
			}
		}
	}
	
	@Test
	public void testOptionalTaskConfigs() throws Exception {
		KafkaGremlinSinkConnector connector = new KafkaGremlinSinkConnector();
		
		Map<String, String> configuration = new HashMap<String, String>();

		configuration.put(Keys.HOST, "host" + UUID.randomUUID().toString());
		configuration.put(Keys.PORT, "port-" + UUID.randomUUID().toString());
		configuration.put(Keys.DATABASE, "database-" + UUID.randomUUID().toString());
		configuration.put(Keys.CONTAINER, "container-" + UUID.randomUUID().toString());
		configuration.put(Keys.KEY, "key-" + UUID.randomUUID().toString());
		connector.start(configuration);
		
		List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
		assertEquals(1, taskConfigs.size(), "Expected exactly one configurations because only one task configurations was requested");
		
		// Verify that optional keys are not present
		assertFalse(taskConfigs.get(0).containsKey(Keys.TRAVERSAL), "Expected to not find " + Keys.TRAVERSAL + " because it was not specified in connector configuration");
		assertFalse(taskConfigs.get(0).containsKey(Keys.ENABLE_SKIP_ON_CONFLICT), "Expected to not find " + Keys.ENABLE_SKIP_ON_CONFLICT + " because it was not specified in connector configuration");
		assertFalse(taskConfigs.get(0).containsKey(Keys.ENABLE_SSL), "Expected to not find " + Keys.ENABLE_SSL + " because it was not specified in connector configuration");
		assertFalse(taskConfigs.get(0).containsKey(Keys.MAX_WAIT_FOR_CONNECTION_MILLISECONDS), "Expected to not find " + Keys.MAX_WAIT_FOR_CONNECTION_MILLISECONDS + " because it was not specified in connector configuration");
		assertFalse(taskConfigs.get(0).containsKey(Keys.RECORD_WRITE_RETRY_COUNT), "Expected to not find " + Keys.RECORD_WRITE_RETRY_COUNT + " because it was not specified in connector configuration");
		assertFalse(taskConfigs.get(0).containsKey(Keys.RECORD_WRITE_RETRY_MILLISECONDS), "Expected to not find " + Keys.RECORD_WRITE_RETRY_MILLISECONDS + " because it was not specified in connector configuration");

		// Set empty values
		configuration.put(Keys.TRAVERSAL, "");
		configuration.put(Keys.ENABLE_SKIP_ON_CONFLICT, "");
		configuration.put(Keys.ENABLE_SSL, "");
		configuration.put(Keys.MAX_WAIT_FOR_CONNECTION_MILLISECONDS, "");
		configuration.put(Keys.RECORD_WRITE_RETRY_COUNT, "");
		configuration.put(Keys.RECORD_WRITE_RETRY_MILLISECONDS, "");
		connector.start(configuration);
		
		taskConfigs = connector.taskConfigs(1);
		assertEquals(1, taskConfigs.size(), "Expected exactly one configurations because only one task configurations was requested subsequently");
		
		// Verify that optional keys are not present
		assertFalse(taskConfigs.get(0).containsKey(Keys.TRAVERSAL), "Expected to not find " + Keys.TRAVERSAL + " because it was not set to empty string");
		assertFalse(taskConfigs.get(0).containsKey(Keys.ENABLE_SKIP_ON_CONFLICT), "Expected to not find " + Keys.ENABLE_SKIP_ON_CONFLICT + " because it was not set to empty string");
		assertFalse(taskConfigs.get(0).containsKey(Keys.ENABLE_SSL), "Expected to not find " + Keys.ENABLE_SSL + " because it was not set to empty string");
		assertFalse(taskConfigs.get(0).containsKey(Keys.MAX_WAIT_FOR_CONNECTION_MILLISECONDS), "Expected to not find " + Keys.MAX_WAIT_FOR_CONNECTION_MILLISECONDS + " because it was not set to empty string");
		assertFalse(taskConfigs.get(0).containsKey(Keys.RECORD_WRITE_RETRY_COUNT), "Expected to not find " + Keys.RECORD_WRITE_RETRY_COUNT + " because it was not set to empty string");
		assertFalse(taskConfigs.get(0).containsKey(Keys.RECORD_WRITE_RETRY_MILLISECONDS), "Expected to not find " + Keys.RECORD_WRITE_RETRY_MILLISECONDS + " because it was not set to empty string");
	}

	@Test
	public void testStop() {
		KafkaGremlinSinkConnector connector = new KafkaGremlinSinkConnector();
		connector.stop();
	}
}
