/**
 * 
 */
package com.microsoft.cosmos.gremlin;

import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Host;
import org.apache.tinkerpop.gremlin.driver.LoadBalancingStrategy;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;

/**
 * In Microsoft Azure Cosmos DB there is only a single host (DNS CNAME) that
 * points to a single VIP behind which there are many physical nodes serving
 * traffic. It is incorrect to mark host as unavailable and look for a better
 * one because there won't be a better one. VIP does not go down, but a single
 * node can. Failure of a node should not render entire cluster unusable We need
 * load balancing strategy that sticks to a single host no matter what.
 * 
 * * @author olignat
 *
 */
final class StickyLoadBalancingStrategy implements LoadBalancingStrategy {
	private final CopyOnWriteArrayList<Host> hosts = new CopyOnWriteArrayList<Host>();
	private final AtomicInteger index = new AtomicInteger();

	@Override
	public void initialize(final Cluster cluster, final Collection<Host> hosts) {
		this.hosts.addAll(hosts);
		this.index.set(new Random().nextInt(Math.max(hosts.size(), 1)));
	}

	@Override
	public Iterator<Host> select(final RequestMessage msg) {
		final int startIndex = index.getAndIncrement();

		if (startIndex > Integer.MAX_VALUE - 10000)
			index.set(0);

		return new Iterator<Host>() {

			private int currentIndex = startIndex;

			@Override
			public boolean hasNext() {
				return hosts.size() > 0;
			}

			@Override
			public Host next() {
				int c = currentIndex++ % hosts.size();
				if (c < 0)
					c += hosts.size();
				return hosts.get(c);
			}
		};
	}

	@Override
	public void onAvailable(final Host host) {
	}

	@Override
	public void onUnavailable(final Host host) {
	}

	@Override
	public void onNew(final Host host) {
		this.hosts.addIfAbsent(host);
	}

	@Override
	public void onRemove(final Host host) {
		this.hosts.remove(host);
	}
}
