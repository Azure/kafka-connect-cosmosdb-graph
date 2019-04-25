/**
 * 
 */
package com.microsoft.cosmos.gremlin;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Host;
import org.junit.jupiter.api.Test;

/**
 * Test coverage for load balancing strategy for Microsoft Azure Cosmos DB
 * 
 * @author olignat
 *
 */
final public class StickyLoadBalancingStrategyTest {

	@Test
	public void testUnavailable() throws Exception {
		StickyLoadBalancingStrategy strategy = new StickyLoadBalancingStrategy();
		
		Cluster cluster = Cluster.build().addContactPoint("localhost").create();
		cluster.init();
	
		strategy.initialize(cluster, cluster.allHosts());
		
		Iterator<Host> iterator = strategy.select(null);
		assertNotNull(iterator.next(), "Expected iterator to return single available host");
		assertNotNull(iterator.next(), "Expected iterator to return single available host on subsequent call");

		// Notify strategy that host is down
		strategy.onUnavailable(cluster.allHosts().iterator().next());

		iterator = strategy.select(null);
		assertNotNull(iterator.next(), "Expected iterator to return single available host after unavailability notification");
		assertNotNull(iterator.next(), "Expected iterator to return single available host on subsequent call after unavailability notification");

		// Notify strategy that host is up
		strategy.onAvailable(cluster.allHosts().iterator().next());

		iterator = strategy.select(null);
		assertNotNull(iterator.next(), "Expected iterator to return single available host after availability notification");
		assertNotNull(iterator.next(), "Expected iterator to return single available host on subsequent call after availability notification");
	}
	
	@Test
	public void testNewRemove() throws Exception {
		StickyLoadBalancingStrategy strategy = new StickyLoadBalancingStrategy();
		
		Cluster cluster = Cluster.build().addContactPoint("localhost").create();
		cluster.init();
	
		// Start with empty collection
		strategy.initialize(cluster, new ArrayList<Host>());
		
		Iterator<Host> iterator = strategy.select(null);
		assertFalse(iterator.hasNext(), "Expected to not have a host");
		assertFalse(iterator.hasNext(), "Expected to not have a host on subsequent call");
		
		// Add a host
		strategy.onNew(cluster.allHosts().iterator().next());

		iterator = strategy.select(null);
		assertTrue(iterator.hasNext(), "Expected to have a host after registration");
		assertTrue(iterator.hasNext(), "Expected to have a host on subsequent call after registration");

		// Remove a host
		strategy.onRemove(cluster.allHosts().iterator().next());

		iterator = strategy.select(null);
		assertFalse(iterator.hasNext(), "Expected to not have a host after removal");
		assertFalse(iterator.hasNext(), "Expected to have a host on subsequent call after removal");
	}	
}
