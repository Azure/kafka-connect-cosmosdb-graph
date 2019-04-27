/**
 * 
 */
package com.microsoft.cosmos.gremlin;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

/**
 * Test coverage for gremlin query builder
 * 
 * @author olignat
 *
 */
final public class GremlinQueryBuilderTest {

	@Test
	public void testParameterizeNull() throws Exception {
		GremlinQueryBuilder.GremlinParameterizedQuery parameterizedQuery = GremlinQueryBuilder.parameterize(null);
		assertNotNull(parameterizedQuery, "Expected to receive a instance of parameterized query for null input traversal");
		assertNull(parameterizedQuery.getParameterizedTraversal(), "Expected to receive null parameterized traversal");
		assertNotNull(parameterizedQuery.getTraversalParameters(), "Expected to receive an instance of traversal parameters");
		assertEquals(0, parameterizedQuery.getTraversalParameters().size(), "Traversal parameters are expected to be empty because there is no traversal on input");
	}

	@Test
	public void testParameterizeNonMatchingPlaceholder() throws Exception {
		GremlinQueryBuilder.GremlinParameterizedQuery parameterizedQuery = GremlinQueryBuilder.parameterize("test123 ${placeholder xyz");
		assertEquals("test123 ${placeholder xyz", parameterizedQuery.getParameterizedTraversal(),
				"Expected placeholder to not be reset because it doesn't have } marker");
		assertNotNull(parameterizedQuery.getTraversalParameters(), "Expected to have an instance of traversal parameters map regardless of the content");
		assertEquals(0, parameterizedQuery.getTraversalParameters().size(), "Traversal parameters are expected to be empty because there are no matching parameter placeholders");
		
		parameterizedQuery = GremlinQueryBuilder.parameterize("test123 placeholder} xyz");
		assertEquals("test123 placeholder} xyz", parameterizedQuery.getParameterizedTraversal(),
				"Expected placeholder to not be reset because it doesn't have { marker");
		assertNotNull(parameterizedQuery.getTraversalParameters(), "Expected to have an instance of traversal parameters map regardless of the content");
		assertEquals(0, parameterizedQuery.getTraversalParameters().size(), "Traversal parameters are expected to be empty because there are no matching parameter placeholders");

		parameterizedQuery = GremlinQueryBuilder.parameterize("test123 {placeholder} xyz");
		assertEquals("test123 {placeholder} xyz", parameterizedQuery.getParameterizedTraversal(),
				"Expected placeholder to not be reset because it doesn't have $ marker");
		assertNotNull(parameterizedQuery.getTraversalParameters(), "Expected to have an instance of traversal parameters map regardless of the content");
		assertEquals(0, parameterizedQuery.getTraversalParameters().size(), "Traversal parameters are expected to be empty because there are no matching parameter placeholders");
	}

	@Test
	public void testParameterizeMultipleMatchingPlaceholders() throws Exception {
		GremlinQueryBuilder.GremlinParameterizedQuery parameterizedQuery = GremlinQueryBuilder.parameterize("test123 ${p1} Y ${2} xyz ${} ---");
		assertEquals("test123 gp1 Y gp2 xyz gp3 ---", parameterizedQuery.getParameterizedTraversal(),
				"Expected 3 placeholders to be replaced by parameter markers");
		assertNotNull(parameterizedQuery.getTraversalParameters(), "Expected to have an instance of traversal parameters map");
		assertEquals(3, parameterizedQuery.getTraversalParameters().size(), "Traversal parameters are expected to be stored in traversal map");

		assertTrue(parameterizedQuery.getTraversalParameters().containsKey("gp1"), "Expected to find the first gremlin parameter inside traversal map");
		assertEquals("p1", parameterizedQuery.getTraversalParameters().get("gp1"), "Expected the first Gremlin parameter to match the first Kafka parameter");
		
		assertTrue(parameterizedQuery.getTraversalParameters().containsKey("gp2"), "Expected to find the second gremlin parameter inside traversal map");
		assertEquals("2", parameterizedQuery.getTraversalParameters().get("gp2"), "Expected the second Gremlin parameter to match the second Kafka parameter");
		
		assertTrue(parameterizedQuery.getTraversalParameters().containsKey("gp3"), "Expected to find the third gremlin parameter inside traversal map");
		assertEquals("", parameterizedQuery.getTraversalParameters().get("gp3"), "Expected the third Gremlin parameter to match the third Kafka parameter");
	}

	@Test
	public void testReplacePrimitiveKey() throws Exception {
		GremlinQueryBuilder.GremlinParameterizedQuery parameterizedQuery = GremlinQueryBuilder.parameterize("before ${key} after");
		assertNotNull(parameterizedQuery, "Expected a valid instance of parameterized query");

		assertEquals("before gp1 after", parameterizedQuery.getParameterizedTraversal(), "Unexpected parameterized Gremlin traversal");
		assertEquals(1, parameterizedQuery.getTraversalParameters().size(), "Expected exactly one key to be stored in traversal parameters");
		
		assertTrue(parameterizedQuery.getTraversalParameters().containsKey("gp1"), "Expected Gremlin parameter was not found in traversal map");
		assertEquals("key", parameterizedQuery.getTraversalParameters().get("gp1"), "Unexpected Kafka placeholder value for Gremlin parameter");
		
		// Testing integer value
		SinkRecord sinkRecord = new SinkRecord("Test-Topic", 1, Schema.INT32_SCHEMA, 1, null, null, 15L);
		Map<String, Object> materializedParameters = GremlinQueryBuilder.materialize(parameterizedQuery, sinkRecord);

		assertNotNull(materializedParameters, "Expected a valid instance of materialized parameters for integer type");
		assertEquals(1, materializedParameters.size(), "Expected a single entry in materialized parameters map because there is a single integer parameter to materialize");

		assertTrue(materializedParameters.containsKey("gp1"), "Expected materialized parameters to contain a single first Gremlin parameter");
		assertEquals(1, materializedParameters.get("gp1"), "Unexpected value of a materialized integer Gremlin parameter");

		// Testing string value
		sinkRecord = new SinkRecord("Test-Topic", 1, Schema.STRING_SCHEMA, "abc", null, null, 15L);
		materializedParameters = GremlinQueryBuilder.materialize(parameterizedQuery, sinkRecord);

		assertNotNull(materializedParameters, "Expected a valid instance of materialized parameters for string type");
		assertEquals(1, materializedParameters.size(), "Expected a single entry in materialized parameters map because there is a single string parameter to materialize");

		assertTrue(materializedParameters.containsKey("gp1"), "Expected materialized parameters to contain a single first Gremlin parameter");
		assertEquals("abc", materializedParameters.get("gp1"), "Unexpected value of a materialized string Gremlin parameter");
	}

	@Test
	public void testReplacePrimitiveValue() throws Exception {
		GremlinQueryBuilder.GremlinParameterizedQuery parameterizedQuery = GremlinQueryBuilder.parameterize("before ${value} after");
		assertNotNull(parameterizedQuery, "Expected a valid instance of parameterized query");
		
		assertEquals("before gp1 after", parameterizedQuery.getParameterizedTraversal(), "Unexpected parameterized Gremlin traversal");
		assertEquals(1, parameterizedQuery.getTraversalParameters().size(), "Expected exactly one key to be stored in traversal parameters");
		
		assertTrue(parameterizedQuery.getTraversalParameters().containsKey("gp1"), "Expected Gremlin parameter was not found in traversal map");
		assertEquals("value", parameterizedQuery.getTraversalParameters().get("gp1"), "Unexpected Kafka placeholder value for Gremlin parameter");		

		// Testing integer value 
		SinkRecord sinkRecord = new SinkRecord("Test-Topic", 1, null, null, Schema.INT32_SCHEMA, 1234, 15L);
		Map<String, Object> materializedParameters = GremlinQueryBuilder.materialize(parameterizedQuery, sinkRecord);

		assertNotNull(materializedParameters, "Expected a valid instance of materialized parameters for integer type");
		assertEquals(1, materializedParameters.size(), "Expected a single entry in materialized parameters map because there is a single integer parameter to materialize");

		assertTrue(materializedParameters.containsKey("gp1"), "Expected materialized parameters to contain a single first Gremlin parameter");
		assertEquals(1234, materializedParameters.get("gp1"), "Unexpected value of a materialized integer Gremlin parameter");

		// Testing integer value 
		sinkRecord = new SinkRecord("Test-Topic", 1, null, null, Schema.STRING_SCHEMA, "xyz", 15L);
		materializedParameters = GremlinQueryBuilder.materialize(parameterizedQuery, sinkRecord);

		assertNotNull(materializedParameters, "Expected a valid instance of materialized parameters for string type");
		assertEquals(1, materializedParameters.size(), "Expected a single entry in materialized parameters map because there is a single string parameter to materialize");

		assertTrue(materializedParameters.containsKey("gp1"), "Expected materialized parameters to contain a single first Gremlin parameter");
		assertEquals("xyz", materializedParameters.get("gp1"), "Unexpected value of a materialized string Gremlin parameter");

		// Test null string value
		sinkRecord = new SinkRecord("Test-Topic", 1, null, null, Schema.STRING_SCHEMA, null, 15L);
		materializedParameters = GremlinQueryBuilder.materialize(parameterizedQuery, sinkRecord);

		assertNotNull(materializedParameters, "Expected a valid instance of materialized parameters for null string type");
		assertEquals(1, materializedParameters.size(), "Expected a single entry in materialized parameters map because there is a single null string parameter to materialize");

		assertTrue(materializedParameters.containsKey("gp1"), "Expected materialized parameters to contain a single first Gremlin parameter");
		assertEquals(null, materializedParameters.get("gp1"), "Unexpected value of a materialized null string Gremlin parameter");
	}

	@Test
	public void testReplaceMapValue() throws Exception {
		GremlinQueryBuilder.GremlinParameterizedQuery parameterizedQuery = GremlinQueryBuilder.parameterize("first ${value.firstField} second ${value.secondField} third '${value.thirdField}' end");
		assertNotNull(parameterizedQuery, "Expected a valid instance of parameterized query");

		assertEquals("first gp1 second gp2 third 'gp3' end", parameterizedQuery.getParameterizedTraversal(), "Unexpected parameterized Gremlin traversal");
		assertEquals(3, parameterizedQuery.getTraversalParameters().size(), "Expected exactly 3 keys to be stored in traversal parameters because there are 3 valid placeholders in the traversal template");
		
		assertTrue(parameterizedQuery.getTraversalParameters().containsKey("gp1"), "Expected first Gremlin parameter was not found in traversal map");
		assertEquals("value.firstField", parameterizedQuery.getTraversalParameters().get("gp1"), "Unexpected Kafka placeholder value for the first Gremlin parameter");		

		assertTrue(parameterizedQuery.getTraversalParameters().containsKey("gp2"), "Expected second Gremlin parameter was not found in traversal map");
		assertEquals("value.secondField", parameterizedQuery.getTraversalParameters().get("gp2"), "Unexpected Kafka placeholder value for the second Gremlin parameter");		

		assertTrue(parameterizedQuery.getTraversalParameters().containsKey("gp3"), "Expected third Gremlin parameter was not found in traversal map");
		assertEquals("value.thirdField", parameterizedQuery.getTraversalParameters().get("gp3"), "Unexpected Kafka placeholder value for the third Gremlin parameter");		
		
		Schema valueSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();

		Map<String, Integer> valueMap = new HashMap<String, Integer>();
		valueMap.put("firstField", 2019);
		valueMap.put("secondField", 423);
		valueMap.put("thirdField", null);

		SinkRecord sinkRecord = new SinkRecord("Test-Topic", 1, null, null, valueSchema, valueMap, 11L);
		Map<String, Object> materializedParameters = GremlinQueryBuilder.materialize(parameterizedQuery, sinkRecord);

		assertNotNull(materializedParameters, "Expected a valid instance of materialized parameters");
		assertEquals(3, materializedParameters.size(), "Expected materialized parameters map to have the same number of entries as there are placeholders in traversal template");

		assertTrue(materializedParameters.containsKey("gp1"), "Expected materialized parameters to contain the first Gremlin parameter");
		assertEquals(2019, materializedParameters.get("gp1"), "Unexpected value of the first materialized Gremlin parameter");

		assertTrue(materializedParameters.containsKey("gp2"), "Expected materialized parameters to contain the second Gremlin parameter");
		assertEquals(423, materializedParameters.get("gp2"), "Unexpected value of the second materialized Gremlin parameter");

		assertTrue(materializedParameters.containsKey("gp3"), "Expected materialized parameters to contain the third Gremlin parameter");
		assertEquals(null, materializedParameters.get("gp3"), "Unexpected value of the third materialized Gremlin parameter");
	}

	@Test
	public void testReplaceStructValue() throws Exception {
		GremlinQueryBuilder.GremlinParameterizedQuery parameterizedQuery = GremlinQueryBuilder.parameterize("first |${key.boolField1}| second {${key.intField2}} third ${key.stringField3} end");
		assertNotNull(parameterizedQuery, "Expected a valid instance of parameterized query");
		
		assertEquals("first |gp1| second {gp2} third gp3 end", parameterizedQuery.getParameterizedTraversal(), "Unexpected parameterized Gremlin traversal");
		assertEquals(3, parameterizedQuery.getTraversalParameters().size(), "Expected exactly 3 keys to be stored in traversal parameters because there are 3 valid placeholders in the traversal template");
		
		assertTrue(parameterizedQuery.getTraversalParameters().containsKey("gp1"), "Expected first Gremlin parameter was not found in traversal map");
		assertEquals("key.boolField1", parameterizedQuery.getTraversalParameters().get("gp1"), "Unexpected Kafka placeholder value for the first Gremlin parameter");		

		assertTrue(parameterizedQuery.getTraversalParameters().containsKey("gp2"), "Expected second Gremlin parameter was not found in traversal map");
		assertEquals("key.intField2", parameterizedQuery.getTraversalParameters().get("gp2"), "Unexpected Kafka placeholder value for the second Gremlin parameter");		

		assertTrue(parameterizedQuery.getTraversalParameters().containsKey("gp3"), "Expected third Gremlin parameter was not found in traversal map");
		assertEquals("key.stringField3", parameterizedQuery.getTraversalParameters().get("gp3"), "Unexpected Kafka placeholder value for the third Gremlin parameter");		
		
		Schema keySchema = SchemaBuilder
				.struct()
				.field("boolField1", Schema.BOOLEAN_SCHEMA)
				.field("intField2", Schema.INT32_SCHEMA)
				.field("stringField3", Schema.STRING_SCHEMA)
				.field("nullField4", Schema.OPTIONAL_STRING_SCHEMA).build();

		Struct keyRecord = new Struct(keySchema)
				.put("boolField1", true)
				.put("intField2", 64534)
				.put("stringField3", "StringValue")
				.put("nullField4", null);

		SinkRecord sinkRecord = new SinkRecord("Test-Topic", 1, keySchema, keyRecord, null, null, 11L);
		Map<String, Object> materializedParameters = GremlinQueryBuilder.materialize(parameterizedQuery, sinkRecord);

		assertNotNull(materializedParameters, "Expected a valid instance of materialized parameters");
		assertEquals(3, materializedParameters.size(), "Expected materialized parameters map to have the same number of entries as there are placeholders in traversal template");

		assertTrue(materializedParameters.containsKey("gp1"), "Expected materialized parameters to contain the first Gremlin parameter");
		assertEquals(true, materializedParameters.get("gp1"), "Unexpected value of the first materialized Gremlin parameter");

		assertTrue(materializedParameters.containsKey("gp2"), "Expected materialized parameters to contain the second Gremlin parameter");
		assertEquals(64534, materializedParameters.get("gp2"), "Unexpected value of the second materialized Gremlin parameter");

		assertTrue(materializedParameters.containsKey("gp3"), "Expected materialized parameters to contain the third Gremlin parameter");
		assertEquals("StringValue", materializedParameters.get("gp3"), "Unexpected value of the third materialized Gremlin parameter");
	}

	@Test
	public void testReplaceArrayValue() throws Exception {
		GremlinQueryBuilder.GremlinParameterizedQuery parameterizedQuery = GremlinQueryBuilder.parameterize("a ${value[0]} b /${value[1]}/ c |${value[2]}| d");
		assertNotNull(parameterizedQuery, "Expected a valid instance of parameterized query");
		
		assertEquals("a gp1 b /gp2/ c |gp3| d", parameterizedQuery.getParameterizedTraversal(), "Unexpected parameterized Gremlin traversal");
		assertEquals(3, parameterizedQuery.getTraversalParameters().size(), "Expected exactly 3 keys to be stored in traversal parameters because there are 3 valid placeholders in the traversal template");

		assertTrue(parameterizedQuery.getTraversalParameters().containsKey("gp1"), "Expected first Gremlin parameter was not found in traversal map");
		assertEquals("value[0]", parameterizedQuery.getTraversalParameters().get("gp1"), "Unexpected Kafka placeholder value for the first Gremlin parameter");		

		assertTrue(parameterizedQuery.getTraversalParameters().containsKey("gp2"), "Expected second Gremlin parameter was not found in traversal map");
		assertEquals("value[1]", parameterizedQuery.getTraversalParameters().get("gp2"), "Unexpected Kafka placeholder value for the second Gremlin parameter");		

		assertTrue(parameterizedQuery.getTraversalParameters().containsKey("gp3"), "Expected third Gremlin parameter was not found in traversal map");
		assertEquals("value[2]", parameterizedQuery.getTraversalParameters().get("gp3"), "Unexpected Kafka placeholder value for the third Gremlin parameter");		
		
		Schema valueSchema = SchemaBuilder.array(Schema.STRING_SCHEMA).build();

		List<String> valueRecord = new ArrayList<String>();
		valueRecord.add("first");
		valueRecord.add("second");
		valueRecord.add(null);

		SinkRecord sinkRecord = new SinkRecord("Test-Topic", 1, null, null, valueSchema, valueRecord, 11L);
		Map<String, Object> materializedParameters = GremlinQueryBuilder.materialize(parameterizedQuery, sinkRecord);

		assertNotNull(materializedParameters, "Expected a valid instance of materialized parameters");
		assertEquals(3, materializedParameters.size(), "Expected materialized parameters map to have the same number of entries as there are placeholders in traversal template");

		assertTrue(materializedParameters.containsKey("gp1"), "Expected materialized parameters to contain the first Gremlin parameter");
		assertEquals("first", materializedParameters.get("gp1"), "Unexpected value of the first materialized Gremlin parameter");

		assertTrue(materializedParameters.containsKey("gp2"), "Expected materialized parameters to contain the second Gremlin parameter");
		assertEquals("second", materializedParameters.get("gp2"), "Unexpected value of the second materialized Gremlin parameter");

		assertTrue(materializedParameters.containsKey("gp3"), "Expected materialized parameters to contain the third Gremlin parameter");
		assertEquals(null, materializedParameters.get("gp3"), "Unexpected value of the third materialized Gremlin parameter");
	}
}
