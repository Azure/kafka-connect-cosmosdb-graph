/**
 * 
 */
package com.microsoft.cosmos.gremlin;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;

/**
 * Test sink task context
 * 
 * @author olignat
 *
 */
public class TestSinkTaskContext implements SinkTaskContext {

	@Override
	public Map<String, String> configs() {
		return null;
	}

	@Override
	public void offset(Map<TopicPartition, Long> offsets) {
	}

	@Override
	public void offset(TopicPartition tp, long offset) {
	}

	@Override
	public void timeout(long timeoutMs) {
	}

	@Override
	public Set<TopicPartition> assignment() {
		return null;
	}

	@Override
	public void pause(TopicPartition... partitions) {
	}

	@Override
	public void resume(TopicPartition... partitions) {
	}

	@Override
	public void requestCommit() {
	}

}
