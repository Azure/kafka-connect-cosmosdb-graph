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
	public void testNullOrEmptyTraversalOrRecord() throws Exception {
		assertNull(GremlinQueryBuilder.build(null, null),
				"Expected null traversal when both traversal and record are null");
		assertEquals("", GremlinQueryBuilder.build("", null), "Expected empty traversal when traversal is empty");
		assertEquals("test321", GremlinQueryBuilder.build("test321", null),
				"Expected traversal to be unmodified when record is null");
		assertEquals("test123 ", GremlinQueryBuilder.build("test123 ${placeholder}", null),
				"Expected traversal to reset without placeholders when record is null");
	}

	@Test
	public void testNonMatchingPlaceholder() throws Exception {
		assertEquals("test123 ${placeholder xyz", GremlinQueryBuilder.build("test123 ${placeholder xyz", null),
				"Expected placeholder to not be reset because it doesn't have } marker");
		assertEquals("test123 placeholder} xyz", GremlinQueryBuilder.build("test123 placeholder} xyz", null),
				"Expected placeholder to not be reset because it doesn't have { marker");
		assertEquals("test123 {placeholder} xyz", GremlinQueryBuilder.build("test123 {placeholder} xyz", null),
				"Expected placeholder to not be reset because it doesn't have $ marker");
	}

	@Test
	public void testMultipleMatchingPlaceholders() throws Exception {
		assertEquals("test123  Y  xyz  ---", GremlinQueryBuilder.build("test123 ${p1} Y ${2} xyz ${} ---", null),
				"Expected 3 placeholders to be reset");
	}

	@Test
	public void testReplacePrimitiveKey() throws Exception {
		SinkRecord sinkRecord = new SinkRecord("Test-Topic", 1, Schema.INT32_SCHEMA, 1, null, null, 15L);
		assertEquals("before 1 after", GremlinQueryBuilder.build("before ${key} after", sinkRecord),
				"Expected primitive integer placeholder to be updated");

		sinkRecord = new SinkRecord("Test-Topic", 1, Schema.INT32_SCHEMA, null, null, null, 15L);
		assertEquals("before  after", GremlinQueryBuilder.build("before ${key} after", sinkRecord),
				"Expected primitive null placeholder to be updated");

		sinkRecord = new SinkRecord("Test-Topic", 1, Schema.STRING_SCHEMA, "abc", null, null, 15L);
		assertEquals("before 'abc' after", GremlinQueryBuilder.build("before '${key}' after", sinkRecord),
				"Expected primitive string placeholder to be updated");
	}

	@Test
	public void testReplacePrimitiveValue() throws Exception {
		SinkRecord sinkRecord = new SinkRecord("Test-Topic", 1, null, null, Schema.INT32_SCHEMA, 1234, 15L);
		assertEquals("before 1234 after", GremlinQueryBuilder.build("before ${value} after", sinkRecord),
				"Expected primitive integer placeholder to be updated");

		sinkRecord = new SinkRecord("Test-Topic", 1, null, null, Schema.STRING_SCHEMA, "xyz", 15L);
		assertEquals("before 'xyz' after", GremlinQueryBuilder.build("before '${value}' after", sinkRecord),
				"Expected primitive string placeholder to be updated");

		sinkRecord = new SinkRecord("Test-Topic", 1, null, null, Schema.STRING_SCHEMA, null, 15L);
		assertEquals("before '' after", GremlinQueryBuilder.build("before '${value}' after", sinkRecord),
				"Expected primitive null placeholder to be updated");
	}

	@Test
	public void testReplaceMapValue() throws Exception {

		Schema valueSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();

		Map<String, Integer> valueMap = new HashMap<String, Integer>();
		valueMap.put("field1", 1);
		valueMap.put("field2", 2);
		valueMap.put("field3", null);

		SinkRecord sinkRecord = new SinkRecord("Test-Topic", 1, null, null, valueSchema, valueMap, 11L);

		assertEquals(
				"first  second  third  end", GremlinQueryBuilder
						.build("first ${key.field1} second ${key.field2} third ${key.field3} end", sinkRecord),
				"Expected map key placeholders to be removed");
		assertEquals(
				"first 1 second 2 third  end", GremlinQueryBuilder
						.build("first ${value.field1} second ${value.field2} third ${value.field3} end", sinkRecord),
				"Expected map value placeholders to be updated");
	}

	@Test
	public void testReplaceStructValue() throws Exception {

		Schema valueSchema = SchemaBuilder.struct().field("boolField1", Schema.BOOLEAN_SCHEMA)
				.field("intField2", Schema.INT32_SCHEMA).field("stringField3", Schema.STRING_SCHEMA)
				.field("nullField4", Schema.OPTIONAL_STRING_SCHEMA).build();

		Struct valueRecord = new Struct(valueSchema).put("boolField1", true).put("intField2", 64534)
				.put("stringField3", "StringValue").put("nullField4", null);

		SinkRecord sinkRecord = new SinkRecord("Test-Topic", 1, null, null, valueSchema, valueRecord, 11L);

		assertEquals("first  second  third  end",
				GremlinQueryBuilder.build(
						"first ${key.boolField1} second ${key.intField2} third ${key.stringField3} end", sinkRecord),
				"Expected structure key placeholders to be removed");
		assertEquals("first true second 64534 third StringValue end",
				GremlinQueryBuilder.build(
						"first ${value.boolField1} second ${value.intField2} third ${value.stringField3} end",
						sinkRecord),
				"Expected structure value placeholders to be updated");
	}

	@Test
	public void testReplaceArrayValue() throws Exception {

		Schema valueSchema = SchemaBuilder.array(Schema.STRING_SCHEMA).build();

		List<String> valueRecord = new ArrayList<String>();
		valueRecord.add("first");
		valueRecord.add("second");
		valueRecord.add(null);

		SinkRecord sinkRecord = new SinkRecord("Test-Topic", 1, null, null, valueSchema, valueRecord, 11L);

		assertEquals("first  second", GremlinQueryBuilder.build("first ${key} second", sinkRecord),
				"Expected array key placeholders to be removed");
		assertEquals("first [first, second, null] end", GremlinQueryBuilder.build("first ${value} end", sinkRecord),
				"Expected array value placeholders to be updated");
		assertEquals("a first b /second/ c || d",
				GremlinQueryBuilder.build("a ${value[0]} b /${value[1]}/ c |${value[2]}| d", sinkRecord),
				"Expected array inidex placeholders to be updated");
	}
}
