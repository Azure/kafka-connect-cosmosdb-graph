/**
 * 
 */
package com.microsoft.cosmos.gremlin;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.tinkerpop.gremlin.driver.AuthProperties;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV1d0;
import org.junit.jupiter.api.Test;

/**
 * Test coverage for sink task
 * 
 * Setup instructions for this test: 1. Start CosmosDB.Emulator.exe with
 * /EnableGremlinEndpoint argument 2. In the browser, navigate to
 * https://localhost:8081/_explorer/index.html 3. Click on Explorer tab on the
 * left -> New Collection on the right 4. Create new database
 * "kafka-gremlin-database" and collection "kafka-gremlin-container" with
 * partition key "/gremlinpk"
 * 
 * @author olignat
 *
 */
final public class KafkaGremlinSinkTaskTest {

	private static final String COSMOS_EMULATOR_HOST = "localhost";
	private static final int COSMOS_EMULATOR_PORT = 8901;
	private static final String COSMOS_EMULATOR_DATABASE = "kafka-gremlin-database";
	private static final String COSMOS_EMULATOR_CONTAINER = "kafka-gremlin-container";
	private static final String COSMOS_EMULATOR_CONTAINER_PARTITIONKEY = "gremlinpk";

	/**
	 * This is a well known Cosmos DB local emulator key
	 */
	private static final String COSMOS_EMULATOR_KEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

	@Test
	public void testReflectionContract() throws Exception {
		assertTrue(Modifier.isPublic(KafkaGremlinSinkTask.class.getModifiers()),
				"Sink task must be public to be discoverable by Kafka");
		assertTrue(KafkaGremlinSinkTask.class.getConstructors().length > 0, "Sink task must have a public constructor");
	}

	@Test
	public void testVersion() throws Exception {
		KafkaGremlinSinkTask task = new KafkaGremlinSinkTask();
		assertEquals(task.version(), "2.2.0", "Expected specific version to be returned by task");
	}

	@Test
	public void testStopWithoutStart() throws Exception {
		KafkaGremlinSinkTask task = new KafkaGremlinSinkTask();
		task.stop();
	}

	@Test
	public void testFlushWithoutStart() throws Exception {
		KafkaGremlinSinkTask task = new KafkaGremlinSinkTask();
		task.flush(new HashMap<TopicPartition, OffsetAndMetadata>());
	}

	@Test
	public void testPrimitiveKeyTraversal() throws Exception {
		// Get properties and configure traversal for primitive type
		Map<String, String> taskConfiguration = KafkaGremlinSinkTaskTest.getRequiredConnectionProperties();
		taskConfiguration.put(KafkaGremlinSinkConnector.Keys.TRAVERSAL, "g.addV().property('"
				+ KafkaGremlinSinkTaskTest.COSMOS_EMULATOR_CONTAINER_PARTITIONKEY + "', ${key})");

		// Create task
		KafkaGremlinSinkTask task = new KafkaGremlinSinkTask();
		SinkTaskContext context = new TestSinkTaskContext();
		task.initialize(context);
		task.start(taskConfiguration);

		// Prepare record to traverse
		String property = "gremlin-test-property-" + UUID.randomUUID().toString();
		SinkRecord sinkRecord = new SinkRecord("Test-Topic", 1, Schema.STRING_SCHEMA, property, null, null, 15L);

		Collection<SinkRecord> sinkRecords = new ArrayList<SinkRecord>();
		sinkRecords.add(sinkRecord);

		// Execute a single record traversal
		task.put(sinkRecords);

		// Verify that record is there
		Cluster cluster = KafkaGremlinSinkTaskTest.createCluster();
		Client client = cluster.connect();

		List<Result> results = client.submit("g.V().has('"
				+ KafkaGremlinSinkTaskTest.COSMOS_EMULATOR_CONTAINER_PARTITIONKEY + "', '" + property + "')").all()
				.get();
		assertTrue(results.size() > 0,
				"Expected to find a vertex with the property that was just inserted by the test");

		// Shut down the client
		client.close();
		cluster.close();

		// Stop task
		task.stop();
	}

	@Test
	public void testOptionalConfigurationTraversal() throws Exception {
		// Get properties and configure traversal
		Map<String, String> taskConfiguration = KafkaGremlinSinkTaskTest.getRequiredConnectionProperties();
		taskConfiguration.put(KafkaGremlinSinkConnector.Keys.TRAVERSAL, "g.addV().property('"
				+ KafkaGremlinSinkTaskTest.COSMOS_EMULATOR_CONTAINER_PARTITIONKEY + "', '${value}')");
		taskConfiguration.put(KafkaGremlinSinkConnector.Keys.ENABLE_SKIP_ON_CONFLICT, "true");
		taskConfiguration.put(KafkaGremlinSinkConnector.Keys.ENABLE_ERROR_ON_EMPTY_RESULTS, "true");
		taskConfiguration.put(KafkaGremlinSinkConnector.Keys.MAX_WAIT_FOR_CONNECTION_MILLISECONDS, "5000");
		taskConfiguration.put(KafkaGremlinSinkConnector.Keys.RECORD_WRITE_RETRY_COUNT, "50");
		taskConfiguration.put(KafkaGremlinSinkConnector.Keys.RECORD_WRITE_RETRY_MILLISECONDS, "35");

		taskConfiguration.remove(KafkaGremlinSinkConnector.Keys.ENABLE_SSL);

		// Create task
		KafkaGremlinSinkTask task = new KafkaGremlinSinkTask();
		SinkTaskContext context = new TestSinkTaskContext();
		task.initialize(context);
		task.start(taskConfiguration);
		task.stop();
	}

	@Test
	public void testConflictFailTraversal() throws Exception {
		// Get properties and configure traversal for primitive type
		Map<String, String> taskConfiguration = KafkaGremlinSinkTaskTest.getRequiredConnectionProperties();
		taskConfiguration.put(KafkaGremlinSinkConnector.Keys.TRAVERSAL,
				"g.addV().property('" + KafkaGremlinSinkTaskTest.COSMOS_EMULATOR_CONTAINER_PARTITIONKEY + "', ${key})"
						+ ".property('id', ${value})");

		// Create task
		KafkaGremlinSinkTask task = new KafkaGremlinSinkTask();
		SinkTaskContext context = new TestSinkTaskContext();
		task.initialize(context);
		task.start(taskConfiguration);

		// Prepare record to traverse
		String pkProperty = "gremlin-pk-property-" + UUID.randomUUID().toString();
		String idProperty = "gremlin-id-property-" + UUID.randomUUID().toString();
		SinkRecord sinkRecord = new SinkRecord("Test-Topic", 1, Schema.STRING_SCHEMA, pkProperty, Schema.STRING_SCHEMA,
				idProperty, 15L);

		Collection<SinkRecord> sinkRecords = new ArrayList<SinkRecord>();
		sinkRecords.add(sinkRecord);

		// Execute a single record traversal
		task.put(sinkRecords);

		try {
			// First execution should succeed, second execution must throw an exception
			// because we've added the same identifier
			task.put(sinkRecords);
		} catch (RetriableException e) {

			Throwable currentThrowable = e.getCause();
			assertTrue(currentThrowable instanceof ExecutionException,
					"Expected to see concurrent execution exception");

			currentThrowable = currentThrowable.getCause();
			assertTrue(currentThrowable instanceof ResponseException,
					"Expected to receive a conflicting error because duplicate key was inserted");

			ResponseException responseException = (ResponseException) currentThrowable;

			assertEquals(409, (int) responseException.getStatusAttributes().get().get("x-ms-status-code"),
					"Unexpected response code on conflicting write");
		}

		// Disable failures on conflicting writes
		taskConfiguration.put(KafkaGremlinSinkConnector.Keys.ENABLE_SKIP_ON_CONFLICT, Boolean.TRUE.toString());
		task.start(taskConfiguration);

		// Now this will throw an exception internally and will be caught
		task.put(sinkRecords);

		// Stop task
		task.stop();
	}

	private static Map<String, String> getRequiredConnectionProperties() {
		Map<String, String> taskConfiguration = new HashMap<String, String>();
		taskConfiguration.put(KafkaGremlinSinkConnector.Keys.HOST, KafkaGremlinSinkTaskTest.COSMOS_EMULATOR_HOST);
		taskConfiguration.put(KafkaGremlinSinkConnector.Keys.PORT,
				String.valueOf(KafkaGremlinSinkTaskTest.COSMOS_EMULATOR_PORT));
		taskConfiguration.put(KafkaGremlinSinkConnector.Keys.DATABASE,
				KafkaGremlinSinkTaskTest.COSMOS_EMULATOR_DATABASE);
		taskConfiguration.put(KafkaGremlinSinkConnector.Keys.CONTAINER,
				KafkaGremlinSinkTaskTest.COSMOS_EMULATOR_CONTAINER);
		taskConfiguration.put(KafkaGremlinSinkConnector.Keys.KEY, KafkaGremlinSinkTaskTest.COSMOS_EMULATOR_KEY);
		taskConfiguration.put(KafkaGremlinSinkConnector.Keys.ENABLE_SSL, Boolean.FALSE.toString());

		return taskConfiguration;
	}

	private static Cluster createCluster() throws Exception {
		Cluster.Builder builder = Cluster.build();
		builder.addContactPoint(KafkaGremlinSinkTaskTest.COSMOS_EMULATOR_HOST);
		builder.port(KafkaGremlinSinkTaskTest.COSMOS_EMULATOR_PORT);

		AuthProperties authenticationProperties = new AuthProperties();
		authenticationProperties.with(AuthProperties.Property.USERNAME, String.format("/dbs/%s/colls/%s",
				KafkaGremlinSinkTaskTest.COSMOS_EMULATOR_DATABASE, KafkaGremlinSinkTaskTest.COSMOS_EMULATOR_CONTAINER));
		authenticationProperties.with(AuthProperties.Property.PASSWORD, KafkaGremlinSinkTaskTest.COSMOS_EMULATOR_KEY);

		builder.authProperties(authenticationProperties);
		builder.enableSsl(false);

		Map<String, Object> config = new HashMap<String, Object>();
		config.put("serializeResultToString", "true");

		GraphSONMessageSerializerV1d0 serializer = new GraphSONMessageSerializerV1d0();
		serializer.configure(config, null);

		builder.serializer(serializer);

		return builder.create();
	}
}
