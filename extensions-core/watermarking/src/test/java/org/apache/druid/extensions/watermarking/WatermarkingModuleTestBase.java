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

package org.apache.druid.extensions.watermarking;

import com.google.inject.Injector;
import org.apache.druid.extensions.watermarking.storage.WatermarkSink;
import org.apache.druid.extensions.watermarking.storage.WatermarkSource;
import org.apache.druid.extensions.watermarking.storage.composite.CompositeWatermarkSink;
import org.apache.druid.extensions.watermarking.storage.google.DatastoreWatermarkStore;
import org.apache.druid.extensions.watermarking.storage.google.DatastoreWatermarkStoreConfig;
import org.apache.druid.extensions.watermarking.storage.google.PubsubWatermarkSink;
import org.apache.druid.extensions.watermarking.storage.google.PubsubWatermarkSinkConfig;
import org.apache.druid.extensions.watermarking.storage.memory.MemoryWatermarkStore;
import org.apache.druid.extensions.watermarking.storage.sql.MySqlWatermarkStore;
import org.apache.druid.extensions.watermarking.storage.sql.SqlWatermarkStoreConfig;
import org.junit.Assert;

import java.util.Properties;

public abstract class WatermarkingModuleTestBase
{
  private static final int MAX_HISTORICAL_ENTRIES = 100;

  private final String mysqlHost = "localhost";
  private final String mysqlPort = "32768";
  private final String mysqlUser = "metamx";
  private final String mysqlPass = "supersecret";
  private final String mysqlConnectUri = "jdbc:mysql://localhost:32768/local?characterEncoding=UTF-8";

  private final String testProjectId = "metamarkets-fake";
  private final String testTopic = "watermarking-test";

  Properties getBaseProperties()
  {
    Properties props = new Properties();
    props.setProperty("druid.processing.numThreads", "1");
    props.setProperty("druid.processing.buffer.sizeBytes", Integer.toString(1024 * 10));
    props.setProperty("druid.processing.numMergeBuffers", "1");
    return props;
  }


  void setCache(Properties props)
  {
    props.setProperty("druid.watermarking.cache.enable", "true");
  }

  void setCacheOptionals(Properties props)
  {
    props.setProperty("druid.watermarking.cache.expireMinutes", "10");
    props.setProperty("druid.watermarking.cache.maxSize", "1000");
  }

  void setMemorySink(Properties props)
  {
    props.setProperty("druid.watermarking.sink.type", "memory");
  }

  void setMemorySource(Properties props)
  {
    props.setProperty("druid.watermarking.source.type", "memory");
  }

  void setMemoryStore(Properties props)
  {
    props.setProperty("druid.watermarking.store.memory.maxHistoricalEntries", Integer.toString(MAX_HISTORICAL_ENTRIES));
  }

  void setMysqlSink(Properties props)
  {
    props.setProperty("druid.watermarking.sink.type", "mysql");
  }

  void setMysqlSource(Properties props)
  {
    props.setProperty("druid.watermarking.source.type", "mysql");
  }

  void setMysqlStore(Properties props)
  {
    props.setProperty("druid.watermarking.store.mysql.host", mysqlHost);
    props.setProperty("druid.watermarking.store.mysql.port", mysqlPort);
    props.setProperty("druid.watermarking.store.mysql.user", mysqlUser);
    props.setProperty("druid.watermarking.store.mysql.password", mysqlPass);
    props.setProperty("druid.watermarking.store.mysql.connectURI", mysqlConnectUri);
    props.setProperty("druid.watermarking.store.mysql.createTimelineTables", "true");
  }

  void setDatastoreSink(Properties props)
  {
    props.setProperty("druid.watermarking.sink.type", "datastore");
  }

  void setDatastoreSource(Properties props)
  {
    props.setProperty("druid.watermarking.source.type", "datastore");
  }

  void setDatastoreStore(Properties props)
  {
    props.setProperty("druid.watermarking.store.datastore.projectId", testProjectId);
  }

  void setPubsubSink(Properties props)
  {
    props.setProperty("druid.watermarking.sink.type", "pubsub");
  }

  void setPubsubStore(Properties props)
  {
    props.setProperty("druid.watermarking.store.pubsub.projectId", testProjectId);
    props.setProperty("druid.watermarking.store.pubsub.topic", testTopic);
  }

  void setCompositePubsubSink(Properties props)
  {
    props.setProperty("druid.watermarking.sink.type", "composite");
  }

  void setCompositePubsubStore(Properties props)
  {
    props.setProperty(
        "druid.watermarking.store.composite.sinks",
        "[\"MemoryWatermarkStore\",\"PubsubWatermarkSink\"]"
    );
    setPubsubStore(props);
  }

  void assertMemorySink(WatermarkSink sink)
  {
    Assert.assertTrue(
        "WatermarkSink should be MemoryWatermarkStore",
        sink instanceof MemoryWatermarkStore
    );
  }

  void assertMemorySource(WatermarkSource source)
  {
    Assert.assertTrue(
        "WatermarkSource should be MemoryWatermarkStore",
        source instanceof MemoryWatermarkStore
    );
  }

  void assertMysqlSink(WatermarkSink sink)
  {
    Assert.assertTrue(
        "WatermarkSink should be MySqlWatermarkStore",
        sink instanceof MySqlWatermarkStore
    );
  }

  void assertMysqlSource(WatermarkSource source)
  {
    Assert.assertTrue(
        "WatermarkSource should be MySqlWatermarkStore",
        source instanceof MySqlWatermarkStore
    );
  }

  void assertDatastoreSink(WatermarkSink sink)
  {
    Assert.assertTrue(
        "WatermarkSink should be DatastoreWatermarkStore",
        sink instanceof DatastoreWatermarkStore
    );
  }

  void assertDatastoreSource(WatermarkSource source)
  {
    Assert.assertTrue(
        "WatermarkSource should be DatastoreWatermarkStore",
        source instanceof DatastoreWatermarkStore
    );
  }

  void assertPubsubSink(WatermarkSink sink)
  {
    Assert.assertTrue(
        "WatermarkSink should be PubsubWatermarkStore",
        sink instanceof PubsubWatermarkSink
    );
  }

  void assertCompositeSink(WatermarkSink sink)
  {
    Assert.assertTrue(
        "WatermarkSink should be CompositeWatermarkSink",
        sink instanceof CompositeWatermarkSink
    );
  }

  void assertMysqlConfig(SqlWatermarkStoreConfig config)
  {
    Assert.assertEquals(
        "The config value mysqlHost should be correct",
        mysqlHost, config.getHost()
    );
    Assert.assertEquals(
        "The config value mysqlHost should be correct",
        mysqlPort, Integer.toString(config.getPort())
    );
    Assert.assertEquals(
        "The config value user should be correct",
        mysqlUser, config.getUser()
    );
    Assert.assertEquals(
        "The config value password should be correct",
        mysqlPass, config.getPassword()
    );
    Assert.assertEquals(
        "The config value connectURI should be some ugly string",
        mysqlConnectUri,
        config.getConnectURI()
    );
    Assert.assertFalse("The config value isCreateTables should be false", config.isCreateTables());
    Assert.assertTrue("The config value isCreateTimelineTables should be true", config.isCreateTimelineTables());
  }

  void assertDatastoreConfig(DatastoreWatermarkStoreConfig config)
  {
    Assert.assertEquals(
        "The config value projectId should be correct",
        testProjectId, config.getProjectId()
    );
  }

  void assertPubsubConfig(PubsubWatermarkSinkConfig config)
  {
    Assert.assertEquals(
        "The config value projectId should be correct",
        testProjectId, config.getProjectId()
    );
    Assert.assertEquals(
        "The config value topic should be correct",
        testTopic, config.getTopic()
    );
  }

  void assertCompositePubsubConfig(CompositeWatermarkSink sink)
  {
    boolean hasMemorySink = false;
    boolean hasPubsubSink = false;
    for (WatermarkSink kitchenSink : sink.getSinks()) {
      if (kitchenSink instanceof MemoryWatermarkStore) {
        hasMemorySink = true;
      }
      if (kitchenSink instanceof PubsubWatermarkSink) {
        hasPubsubSink = true;
        assertPubsubConfig(((PubsubWatermarkSink) kitchenSink).getConfig());
      }
    }
    Assert.assertTrue(
        "The sink should have had a MemoryWatermarkStore configured",
        hasMemorySink
    );
    Assert.assertTrue(
        "The sink should have had a PubsubWatermarkSink configured",
        hasPubsubSink
    );
  }

  public abstract Class getType();

  public abstract Injector newInjector(Properties props);
}
