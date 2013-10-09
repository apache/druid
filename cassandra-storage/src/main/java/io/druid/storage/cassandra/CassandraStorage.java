/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.storage.cassandra;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.recipes.storage.CassandraChunkedStorageProvider;
import com.netflix.astyanax.recipes.storage.ChunkedStorageProvider;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 * Superclass for accessing Cassandra Storage.
 * 
 * This is the schema used to support the index and descriptor storage:
 * 
 * CREATE TABLE index_storage ( key text, chunk text, value blob, PRIMARY KEY (key, chunk)) WITH COMPACT STORAGE;
 * CREATE TABLE descriptor_storage ( key varchar, lastModified timestamp, descriptor varchar, PRIMARY KEY (key) ) WITH COMPACT STORAGE;
 */
public class CassandraStorage 
{
	private static final String CLUSTER_NAME = "druid_cassandra_cluster";
	private static final String INDEX_TABLE_NAME = "index_storage";
	private static final String DESCRIPTOR_TABLE_NAME = "descriptor_storage";

	private AstyanaxContext<Keyspace> astyanaxContext;
	final Keyspace keyspace;
	final ChunkedStorageProvider indexStorage;
	final ColumnFamily<String, String> descriptorStorage;	
	final CassandraDataSegmentConfig config;

	public CassandraStorage(CassandraDataSegmentConfig config)
	{
		this.astyanaxContext = new AstyanaxContext.Builder()
		    .forCluster(CLUSTER_NAME)
		    .forKeyspace(config.getKeyspace())
		    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE))
		    .withConnectionPoolConfiguration(
		        new ConnectionPoolConfigurationImpl("MyConnectionPool").setMaxConnsPerHost(10)
		            .setSeeds(config.getHost())).withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
		    .buildKeyspace(ThriftFamilyFactory.getInstance());
		this.astyanaxContext.start();
		this.keyspace = this.astyanaxContext.getEntity();
		this.config = config;
		indexStorage = new CassandraChunkedStorageProvider(keyspace, INDEX_TABLE_NAME);

		descriptorStorage = new ColumnFamily<String, String>(DESCRIPTOR_TABLE_NAME,
		    StringSerializer.get(), StringSerializer.get());
	}
}
