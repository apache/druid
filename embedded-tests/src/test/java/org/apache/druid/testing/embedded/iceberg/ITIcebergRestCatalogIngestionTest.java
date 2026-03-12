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

package org.apache.druid.testing.embedded.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.parquet.ParquetExtensionsModule;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.testing.embedded.msq.EmbeddedMSQApis;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Ingestion test for Iceberg tables via a REST catalog.
 * Exercises the classloader-sensitive {@code DynConstructors} path in
 * {@code RestIcebergCatalog.setupCatalog()} (see #18015, #18017).
 */
public class ITIcebergRestCatalogIngestionTest extends EmbeddedClusterTestBase
{
  private static final String ICEBERG_NAMESPACE = "default";
  private static final String ICEBERG_TABLE_NAME = "test_events";

  private static final Schema ICEBERG_SCHEMA = new Schema(
      Types.NestedField.required(1, "event_time", Types.StringType.get()),
      Types.NestedField.required(2, "name", Types.StringType.get()),
      Types.NestedField.required(3, "value", Types.LongType.get())
  );

  private final File warehouseDir = FileUtils.createTempDir();
  private final IcebergRestCatalogResource icebergCatalog = new IcebergRestCatalogResource(warehouseDir);

  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer()
      .setServerMemory(300_000_000L)
      .addProperty("druid.worker.capacity", "2");
  private final EmbeddedBroker broker = new EmbeddedBroker();

  private EmbeddedMSQApis msqApis;
  private RESTCatalog clientCatalog;

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        .addResource(icebergCatalog)
        .addExtension(ParquetExtensionsModule.class)
        .addServer(overlord)
        .addServer(coordinator)
        .addServer(indexer)
        .addServer(broker)
        .addServer(new EmbeddedHistorical());
  }

  @BeforeAll
  public void setupIcebergTable() throws IOException
  {
    msqApis = new EmbeddedMSQApis(cluster, overlord);

    clientCatalog = createClientCatalog();
    clientCatalog.createNamespace(Namespace.of(ICEBERG_NAMESPACE));

    final TableIdentifier tableId = TableIdentifier.of(ICEBERG_NAMESPACE, ICEBERG_TABLE_NAME);
    final Table table = clientCatalog.createTable(tableId, ICEBERG_SCHEMA);
    writeTestData(table);
  }

  @Test
  public void testIngestFromIcebergRestCatalog()
  {
    final String catalogUri = icebergCatalog.getCatalogUri();

    final String sql = StringUtils.format(
        "INSERT INTO %s\n"
        + "SELECT\n"
        + "  TIME_PARSE(\"event_time\") AS __time,\n"
        + "  \"name\",\n"
        + "  \"value\"\n"
        + "FROM TABLE(\n"
        + "  EXTERN(\n"
        + "    '{\"type\":\"iceberg\","
        + "\"tableName\":\"%s\","
        + "\"namespace\":\"%s\","
        + "\"icebergCatalog\":{\"type\":\"rest\",\"catalogUri\":\"%s\","
        + "\"catalogProperties\":{\"io-impl\":\"org.apache.iceberg.hadoop.HadoopFileIO\"}},"
        + "\"warehouseSource\":{\"type\":\"local\"}}',\n"
        + "    '{\"type\":\"parquet\"}',\n"
        + "    '[{\"type\":\"string\",\"name\":\"event_time\"},"
        + "{\"type\":\"string\",\"name\":\"name\"},"
        + "{\"type\":\"long\",\"name\":\"value\"}]'\n"
        + "  )\n"
        + ")\n"
        + "PARTITIONED BY ALL TIME",
        dataSource,
        ICEBERG_TABLE_NAME,
        ICEBERG_NAMESPACE,
        catalogUri
    );

    final SqlTaskStatus taskStatus = msqApis.submitTaskSql(sql);
    cluster.callApi().waitForTaskToSucceed(taskStatus.getTaskId(), overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    cluster.callApi().verifySqlQuery(
        "SELECT __time, \"name\", \"value\" FROM %s ORDER BY __time",
        dataSource,
        "2024-01-01T00:00:00.000Z,alice,100\n"
        + "2024-01-01T01:00:00.000Z,bob,200\n"
        + "2024-01-01T02:00:00.000Z,charlie,300"
    );
  }

  @AfterAll
  public void tearDownIceberg()
  {
    if (clientCatalog != null) {
      try {
        clientCatalog.dropTable(TableIdentifier.of(ICEBERG_NAMESPACE, ICEBERG_TABLE_NAME));
        clientCatalog.dropNamespace(Namespace.of(ICEBERG_NAMESPACE));
      }
      catch (Exception e) {
        // Best-effort cleanup
      }
      try {
        clientCatalog.close();
      }
      catch (Exception e) {
        // Best-effort cleanup
      }
    }
    org.apache.commons.io.FileUtils.deleteQuietly(warehouseDir);
  }

  private RESTCatalog createClientCatalog()
  {
    // RESTCatalog.initialize() may mutate the properties map, so a mutable map is required
    final Map<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.URI, icebergCatalog.getCatalogUri());
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseDir.getAbsolutePath());
    properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.hadoop.HadoopFileIO");

    final RESTCatalog catalog = new RESTCatalog();
    catalog.setConf(new org.apache.hadoop.conf.Configuration());
    catalog.initialize("test-client", properties);
    return catalog;
  }

  private static void writeTestData(Table table) throws IOException
  {
    final ImmutableList<ImmutableMap<String, Object>> rows = ImmutableList.of(
        ImmutableMap.of("event_time", "2024-01-01T00:00:00.000Z", "name", "alice", "value", 100L),
        ImmutableMap.of("event_time", "2024-01-01T01:00:00.000Z", "name", "bob", "value", 200L),
        ImmutableMap.of("event_time", "2024-01-01T02:00:00.000Z", "name", "charlie", "value", 300L)
    );

    final String filepath = table.location() + "/data/" + UUID.randomUUID() + ".parquet";
    final OutputFile file = table.io().newOutputFile(filepath);

    final DataWriter<GenericRecord> writer;
    try (DataWriter<GenericRecord> w = Parquet.writeData(file)
        .schema(ICEBERG_SCHEMA)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .overwrite()
        .withSpec(table.spec())
        .build()) {
      for (final ImmutableMap<String, Object> row : rows) {
        final GenericRecord record = GenericRecord.create(ICEBERG_SCHEMA);
        record.set(0, row.get("event_time"));
        record.set(1, row.get("name"));
        record.set(2, row.get("value"));
        w.write(record);
      }
      writer = w;
    }

    table.newAppend().appendFile(writer.toDataFile()).commit();
  }
}
