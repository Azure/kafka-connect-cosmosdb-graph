package com.microsoft.cosmos.gremlin;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Class that generates gremlin statements to push data from task into the
 * service.
 *
 * @author olignat
 */
final class GremlinQueryBuilder {

	private static final Logger log = LoggerFactory.getLogger(GremlinQueryBuilder.class);

	private static final String KEY_DESIGNATION = "key";
	private static final String VALUE_DESIGNATION = "value";

	static String build(String traversal, SinkRecord record) {
		if (traversal == null || traversal.isEmpty()) {
			return traversal;
		}

		String processedTraversal = traversal;

		if (record != null) {
			processedTraversal = handleSchemaType(
					handleSchemaType(processedTraversal, record.keySchema(), record.key(),
							GremlinQueryBuilder.KEY_DESIGNATION),
					record.valueSchema(), record.value(), GremlinQueryBuilder.VALUE_DESIGNATION);
		}

		return resetUnmatchedMarkers(processedTraversal);
	}

	private static String handleSchemaType(String traversal, Schema schema, Object value, String prefix) {
		String updatedTraversal = traversal;

		log.debug("Processing query build for prefix '{}', schema type '{}' and value '{}'", prefix,
				schema != null ? schema.type() : "<null schema>", value);

		// Replace primitive type for key
		updatedTraversal = updatedTraversal.replace("${" + prefix + "}", value == null ? "" : value.toString());

		// Process different types of schema
		if (schema != null) {
			if (schema.type() == Type.MAP) {
				// Handle the map
				Map<?, ?> valueMap = (Map<?, ?>) value;
				for (Map.Entry<?, ?> entry : valueMap.entrySet()) {
					Object mapValue = entry.getValue();
					updatedTraversal = updatedTraversal.replace("${" + prefix + "." + entry.getKey() + "}",
							mapValue == null ? "" : mapValue.toString());
				}
			} else if (schema.type() == Type.STRUCT) {
				// Handle structure
				Struct valueStruct = (Struct) value;
				for (Field field : schema.fields()) {
					Object fieldValue = valueStruct.get(field);
					updatedTraversal = updatedTraversal.replace("${" + prefix + "." + field.name() + "}",
							fieldValue == null ? "" : fieldValue.toString());
				}
			} else if (schema.type() == Type.ARRAY) {
				// Handle array
				List<?> valueArray = (List<?>) value;
				for (int i = 0; i < valueArray.size(); i++) {
					Object arrayValue = valueArray.get(i);
					updatedTraversal = updatedTraversal.replace("${" + prefix + "[" + i + "]}",
							arrayValue == null ? "" : arrayValue.toString());
				}
			}
		}

		return updatedTraversal;
	}

	private static String resetUnmatchedMarkers(String traversal) {
		String updatedTraversal = traversal;

		int lastMatchedMarker = -1;
		int lastMatchedMarkerEnd = -1;

		do {
			lastMatchedMarker = updatedTraversal.indexOf("${", lastMatchedMarker + 1);
			if (lastMatchedMarker != -1) {
				lastMatchedMarkerEnd = updatedTraversal.indexOf("}", lastMatchedMarker);
				if (lastMatchedMarkerEnd != -1) {
					updatedTraversal = updatedTraversal.substring(0, lastMatchedMarker)
							+ updatedTraversal.substring(lastMatchedMarkerEnd + 1);
				}
			}
		} while (lastMatchedMarker != -1);

		return updatedTraversal;
	}
}
