/**
 * 
 */
package com.microsoft.cosmos.gremlin;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Modifier;
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
 * Test coverage for sink task 
 * 
 * @author olignat
 *
 */
final public class KafkaGremlinSinkTaskTest {

	@Test
	public void testReflectionContract() throws Exception {
		assertTrue(Modifier.isPublic(KafkaGremlinSinkTask.class.getModifiers()), "Sink task must be public to be discoverable by Kafka");
		assertTrue(KafkaGremlinSinkTask.class.getConstructors().length > 0, "Sink task must have a public constructor");
	}
}
