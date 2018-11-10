/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.storage.cassandra;

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
