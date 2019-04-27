package com.microsoft.cosmos.gremlin;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Class that generates Gremlin statements to push data from task into the
 * service.
 *
 * @author olignat
 */
final class GremlinQueryBuilder {
	private static final Logger log = LoggerFactory.getLogger(GremlinQueryBuilder.class);

	private static final String PARAMETER_MARKER = "gp";
	
	private static final String KEY_DESIGNATION = "key";
	private static final String VALUE_DESIGNATION = "value";
	
	/**
	 * Class that contains results of parameterization of Gremlin traversal before parameter values are materialized
	 * 
	 * @author olignat 
	 */
	static final class GremlinParameterizedQuery {
		private String parameterizedTraversal;
		private Map<String, String> traversalParameters;
		
		GremlinParameterizedQuery(String parameterizedTraversal, Map<String, String> traversalParameters) {
			this.parameterizedTraversal = parameterizedTraversal;
			this.traversalParameters = traversalParameters;
		}
		
		/**
		 * @return modified traversal that contains Kafka parameters replaced with Gremlin parameter markers 
		 */		
		public String getParameterizedTraversal() {
			return this.parameterizedTraversal;
		}

		/**
		 * @return map of Gremlin parameters to Kafka parameters that they replaced
		 * Key is a Gremlin marker (e.g. "gp1")
		 * Value is a Kafka marker without ${} around it (e.g. "value.property") 
		 */
		public Map<String, String> getTraversalParameters() {
			return this.traversalParameters;
		}
	}
	
	/**
	 * Process traversal and replace all Kafka event markers with Gremlin parameter markers.
	 * Extract the map of Kafka to Gremlin parameters to be substituted at a later time during execution. 
	 */
	static GremlinParameterizedQuery parameterize(String traversal) {
		String parameterizedTraversal = traversal;
		Map<String, String> traversalParameters = new HashMap<String, String>();

		if (traversal != null && !traversal.isEmpty()) {
			int lastMatchedMarker = -1;
			int lastMatchedMarkerEnd = -1;
			int parameterCounter = 1;
			
			do {
				// Match parameter
				lastMatchedMarker = parameterizedTraversal.indexOf("${", lastMatchedMarker + 1);
				if (lastMatchedMarker != -1) {
					lastMatchedMarkerEnd = parameterizedTraversal.indexOf("}", lastMatchedMarker);
					if (lastMatchedMarkerEnd != -1) {
						// Extract parameter
						String kafkaParameterMarker = parameterizedTraversal.substring(lastMatchedMarker + 2, lastMatchedMarkerEnd).trim();
						String gremlinParameterMarker =  GremlinQueryBuilder.PARAMETER_MARKER + String.valueOf(parameterCounter++);
						
						// Replace with a Gremlin parameter marker 
						parameterizedTraversal = 
								parameterizedTraversal.substring(0, lastMatchedMarker)
								+ gremlinParameterMarker
								+ parameterizedTraversal.substring(lastMatchedMarkerEnd + 1);
						
						// Capture parameter mapping
						traversalParameters.put(gremlinParameterMarker, kafkaParameterMarker);
					}
				}
			} while (lastMatchedMarker != -1);
		}
		
		log.debug("Parameterized traversal '{}' as '{}' with {} parameters", traversal, parameterizedTraversal, traversalParameters.keySet().size());
		
		return new GremlinQueryBuilder.GremlinParameterizedQuery(parameterizedTraversal, traversalParameters); 
	}
	
	/**
	 * Using parameterized query populate a map of parameters 
	 */
	static Map<String, Object> materialize(GremlinParameterizedQuery parameterizedQuery, SinkRecord record) {
		Map<String, Object> materializedParameters = new HashMap<String, Object>();

		// Check if we have work to do
		if (!parameterizedQuery.getTraversalParameters().isEmpty()) {
			// Iterate through each Gremlin parameter
			for (Map.Entry<String, String> traversalParameter : parameterizedQuery.getTraversalParameters().entrySet()) {
				// Get the references
				String gremlinParameterMarker = traversalParameter.getKey(); 
				String kafkaParameterMarker = traversalParameter.getValue(); 

				Schema parameterSchema = null;
				Object parameterValue = null;
				
				// Resolve schema and value
				if (kafkaParameterMarker.startsWith(GremlinQueryBuilder.KEY_DESIGNATION)) {
					parameterSchema = record.keySchema();
					parameterValue = record.key();
				}
				else if (kafkaParameterMarker.startsWith(GremlinQueryBuilder.VALUE_DESIGNATION)) {
					parameterSchema = record.valueSchema();
					parameterValue = record.value();
				}
				
				// Process different types of schema
				if (parameterSchema != null) {
					// Check if parameter marker includes any child references
					int indexOfChildPropertySeparator = kafkaParameterMarker.indexOf('.');
					
					if (parameterSchema.type() == Type.MAP) {
						// Check if we have a child property reference
						// If we do not - entire map is a fair game
						if (indexOfChildPropertySeparator != -1) {
							// Handle the map
							Map<?, ?> parameterValueMap = (Map<?, ?>) parameterValue;
							parameterValue = parameterValueMap.get(kafkaParameterMarker.substring(indexOfChildPropertySeparator + 1));
						}
					} else if (parameterSchema.type() == Type.STRUCT) {
						// Check if we have a child property reference
						// If we do not - entire map is a fair game
						if (indexOfChildPropertySeparator != -1) {
							// Handle structure
							Struct parameterValueStruct = (Struct) parameterValue;
							parameterValue = parameterValueStruct.get(kafkaParameterMarker.substring(indexOfChildPropertySeparator + 1));
						}
					} else if (parameterSchema.type() == Type.ARRAY) {
						// Check if we have a positional element reference
						int indexOfArrayIndexerStart = kafkaParameterMarker.indexOf('[');
						if (indexOfArrayIndexerStart != -1) {
							// Find the first closing positional element
							int indexOfArrayIndexerEnd = kafkaParameterMarker.indexOf(']', indexOfArrayIndexerStart + 1);
							if (indexOfArrayIndexerEnd > indexOfArrayIndexerStart) {
								// Handle array
								List<?> parameterValueArray = (List<?>) parameterValue;
								parameterValue = parameterValueArray.get(Integer.parseInt(kafkaParameterMarker.substring(indexOfArrayIndexerStart + 1, indexOfArrayIndexerEnd)));
							}
						}
					}
				}
				
				// Store resolver parameter value
				materializedParameters.put(gremlinParameterMarker, parameterValue);
			}
		}
		
		return materializedParameters;
	}
}
