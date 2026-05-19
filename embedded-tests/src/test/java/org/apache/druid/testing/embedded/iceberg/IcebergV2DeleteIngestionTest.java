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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;

/**
 * End-to-end integration test for Iceberg v2 delete file support.
 * Creates v2-format tables with positional and equality deletes,
 * ingests via MSQ SQL, and verifies that deleted rows are excluded.
 */
public class IcebergV2DeleteIngestionTest extends EmbeddedClusterTestBase
{
  private static final String ICEBERG_NAMESPACE = "default";

  private static final Schema TABLE_SCHEMA = new Schema(
      Types.NestedField.required(1, "event_time", Types.StringType.get()),
      Types.NestedField.required(2, "order_id", Types.IntegerType.get()),
      Types.NestedField.required(3, "product", Types.StringType.get()),
      Types.NestedField.required(4, "amount", Types.LongType.get())
  );

  private final IcebergRestCatalogResource icebergCatalog = new IcebergRestCatalogResource();

  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer()
      .setServerMemory(300_000_000L)
      .addProperty("druid.worker.capacity", "2");
  private final EmbeddedBroker broker = new EmbeddedBroker();

  private EmbeddedMSQApis msqApis;

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

  @Override
  @BeforeAll
  public void setup() throws Exception
  {
    super.setup();
    msqApis = new EmbeddedMSQApis(cluster, overlord);
    icebergCatalog.createNamespace(ICEBERG_NAMESPACE);
  }

  @Override
  @AfterAll
  public void tearDown()
  {
    dropTableSafely("eq_delete_test");
    dropTableSafely("pos_delete_test");
    dropTableSafely("mixed_delete_test");
    dropTableSafely("no_delete_test");
    icebergCatalog.dropNamespace(ICEBERG_NAMESPACE);
    super.tearDown();
  }

  @Test
  public void testV2EqualityDeleteIngestion() throws IOException
  {
    final String tableName = "eq_delete_test";
    final Table table = createV2Table(tableName);

    // Append 3 rows: order_id=1, 2, 3
    final DataFile dataFile = writeDataFile(table, ImmutableList.of(
        row("2024-01-01T00:00:00.000Z", 1, "Widget", 100L),
        row("2024-01-01T01:00:00.000Z", 2, "Gadget", 200L),
        row("2024-01-01T02:00:00.000Z", 3, "Doohickey", 300L)
    ));
    table.newAppend().appendFile(dataFile).commit();

    // Write equality delete: delete where order_id=2
    final Schema deleteSchema = new Schema(
        Types.NestedField.required(2, "order_id", Types.IntegerType.get())
    );
    final DeleteFile eqDelete = writeEqualityDeleteFile(table, deleteSchema, 2, ImmutableList.of(
        eqDeleteRow(deleteSchema, 2)
    ));
    table.newRowDelta().addDeletes(eqDelete).commit();

    // Ingest and verify: only order_id=1 and 3 should be present
    ingestAndVerify(
        tableName,
        "SELECT __time, \"order_id\", \"product\", \"amount\" FROM %s ORDER BY __time",
        "2024-01-01T00:00:00.000Z,1,Widget,100\n"
        + "2024-01-01T02:00:00.000Z,3,Doohickey,300"
    );
  }

  @Test
  public void testV2PositionalDeleteIngestion() throws IOException
  {
    final String tableName = "pos_delete_test";
    final Table table = createV2Table(tableName);

    final DataFile dataFile = writeDataFile(table, ImmutableList.of(
        row("2024-01-01T00:00:00.000Z", 1, "Widget", 100L),
        row("2024-01-01T01:00:00.000Z", 2, "Gadget", 200L),
        row("2024-01-01T02:00:00.000Z", 3, "Doohickey", 300L)
    ));
    table.newAppend().appendFile(dataFile).commit();

    // Write positional delete: delete row at position 1 (order_id=2)
    final DeleteFile posDelete = writePositionalDeleteFile(
        table,
        dataFile.location(),
        1L
    );
    table.newRowDelta().addDeletes(posDelete).commit();

    ingestAndVerify(
        tableName,
        "SELECT __time, \"order_id\", \"product\", \"amount\" FROM %s ORDER BY __time",
        "2024-01-01T00:00:00.000Z,1,Widget,100\n"
        + "2024-01-01T02:00:00.000Z,3,Doohickey,300"
    );
  }

  @Test
  public void testV1TableIngestionUnchanged() throws IOException
  {
    final String tableName = "no_delete_test";
    final Table table = createV2Table(tableName);

    // Append 3 rows, no deletes
    final DataFile dataFile = writeDataFile(table, ImmutableList.of(
        row("2024-01-01T00:00:00.000Z", 1, "Widget", 100L),
        row("2024-01-01T01:00:00.000Z", 2, "Gadget", 200L),
        row("2024-01-01T02:00:00.000Z", 3, "Doohickey", 300L)
    ));
    table.newAppend().appendFile(dataFile).commit();

    ingestAndVerify(
        tableName,
        "SELECT __time, \"order_id\", \"product\", \"amount\" FROM %s ORDER BY __time",
        "2024-01-01T00:00:00.000Z,1,Widget,100\n"
        + "2024-01-01T01:00:00.000Z,2,Gadget,200\n"
        + "2024-01-01T02:00:00.000Z,3,Doohickey,300"
    );
  }

  @Test
  public void testV2MixedDeleteIngestion() throws IOException
  {
    final String tableName = "mixed_delete_test";
    final Table table = createV2Table(tableName);

    final DataFile dataFile = writeDataFile(table, ImmutableList.of(
        row("2024-01-01T00:00:00.000Z", 1, "Widget", 100L),
        row("2024-01-01T01:00:00.000Z", 2, "Gadget", 200L),
        row("2024-01-01T02:00:00.000Z", 3, "Doohickey", 300L),
        row("2024-01-01T03:00:00.000Z", 4, "Thingamajig", 400L),
        row("2024-01-01T04:00:00.000Z", 5, "Whatchamacallit", 500L)
    ));
    table.newAppend().appendFile(dataFile).commit();

    // Positional delete: remove position 0 (order_id=1)
    final DeleteFile posDelete = writePositionalDeleteFile(table, dataFile.location(), 0L);
    table.newRowDelta().addDeletes(posDelete).commit();

    // Equality delete: remove order_id=4
    final Schema deleteSchema = new Schema(
        Types.NestedField.required(2, "order_id", Types.IntegerType.get())
    );
    final DeleteFile eqDelete = writeEqualityDeleteFile(table, deleteSchema, 2, ImmutableList.of(
        eqDeleteRow(deleteSchema, 4)
    ));
    table.newRowDelta().addDeletes(eqDelete).commit();

    // Only order_id=2, 3, 5 should remain
    ingestAndVerify(
        tableName,
        "SELECT __time, \"order_id\", \"product\", \"amount\" FROM %s ORDER BY __time",
        "2024-01-01T01:00:00.000Z,2,Gadget,200\n"
        + "2024-01-01T02:00:00.000Z,3,Doohickey,300\n"
        + "2024-01-01T04:00:00.000Z,5,Whatchamacallit,500"
    );
  }

  // --- Helper methods ---

  private Table createV2Table(final String tableName)
  {
    return icebergCatalog.createTable(
        ICEBERG_NAMESPACE,
        tableName,
        TABLE_SCHEMA,
        PartitionSpec.unpartitioned(),
        ImmutableMap.of("format-version", "2")
    );
  }

  private ImmutableMap<String, Object> row(
      final String eventTime,
      final int orderId,
      final String product,
      final long amount
  )
  {
    return ImmutableMap.of(
        "event_time", eventTime,
        "order_id", orderId,
        "product", product,
        "amount", amount
    );
  }

  private DataFile writeDataFile(
      final Table table,
      final ImmutableList<ImmutableMap<String, Object>> rows
  ) throws IOException
  {
    final String filepath = table.location() + "/data/" + UUID.randomUUID() + ".parquet";
    final OutputFile file = table.io().newOutputFile(filepath);

    final DataWriter<GenericRecord> writer = Parquet.writeData(file)
                                                    .schema(TABLE_SCHEMA)
                                                    .createWriterFunc(GenericParquetWriter::create)
                                                    .overwrite()
                                                    .withSpec(table.spec())
                                                    .build();
    try (DataWriter<GenericRecord> w = writer) {
      for (final ImmutableMap<String, Object> row : rows) {
        final GenericRecord record = GenericRecord.create(TABLE_SCHEMA);
        record.set(0, row.get("event_time"));
        record.set(1, row.get("order_id"));
        record.set(2, row.get("product"));
        record.set(3, row.get("amount"));
        w.write(record);
      }
    }
    return writer.toDataFile();
  }

  private GenericRecord eqDeleteRow(final Schema deleteSchema, final int orderId)
  {
    final GenericRecord record = GenericRecord.create(deleteSchema);
    record.setField("order_id", orderId);
    return record;
  }

  private DeleteFile writeEqualityDeleteFile(
      final Table table,
      final Schema deleteSchema,
      final int equalityFieldId,
      final ImmutableList<GenericRecord> deleteRows
  ) throws IOException
  {
    final String deletePath = table.location() + "/data/" + UUID.randomUUID() + "-eq-delete.parquet";
    final OutputFile outputFile = table.io().newOutputFile(deletePath);

    final EqualityDeleteWriter<GenericRecord> writer = Parquet.writeDeletes(outputFile)
                                                             .forTable(table)
                                                             .rowSchema(deleteSchema)
                                                             .createWriterFunc(GenericParquetWriter::create)
                                                             .overwrite()
                                                             .equalityFieldIds(equalityFieldId)
                                                             .buildEqualityWriter();
    try (EqualityDeleteWriter<GenericRecord> w = writer) {
      for (final GenericRecord row : deleteRows) {
        w.write(row);
      }
    }
    return writer.toDeleteFile();
  }

  private DeleteFile writePositionalDeleteFile(
      final Table table,
      final String dataFilePath,
      final long position
  ) throws IOException
  {
    final String deletePath = table.location() + "/data/" + UUID.randomUUID() + "-pos-delete.parquet";
    final OutputFile outputFile = table.io().newOutputFile(deletePath);

    final PositionDeleteWriter<GenericRecord> writer = Parquet.writeDeletes(outputFile)
                                                             .forTable(table)
                                                             .overwrite()
                                                             .buildPositionWriter();
    try (PositionDeleteWriter<GenericRecord> w = writer) {
      final PositionDelete<GenericRecord> posDelete = PositionDelete.create();
      posDelete.set(dataFilePath, position, null);
      w.write(posDelete);
    }
    return writer.toDeleteFile();
  }

  private void ingestAndVerify(
      final String icebergTableName,
      final String verifyQueryTemplate,
      final String expectedCsv
  )
  {
    final String catalogUri = icebergCatalog.getCatalogUri();
    final String druidDataSource = dataSource + "_" + icebergTableName;

    final String sql = StringUtils.format(
        "INSERT INTO \"%s\"\n"
        + "SELECT\n"
        + "  TIME_PARSE(\"event_time\") AS __time,\n"
        + "  \"order_id\",\n"
        + "  \"product\",\n"
        + "  \"amount\"\n"
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
        + "{\"type\":\"long\",\"name\":\"order_id\"},"
        + "{\"type\":\"string\",\"name\":\"product\"},"
        + "{\"type\":\"long\",\"name\":\"amount\"}]'\n"
        + "  )\n"
        + ")\n"
        + "PARTITIONED BY ALL TIME",
        druidDataSource,
        icebergTableName,
        ICEBERG_NAMESPACE,
        catalogUri
    );

    final SqlTaskStatus taskStatus = msqApis.submitTaskSql(sql);
    cluster.callApi().waitForTaskToSucceed(taskStatus.getTaskId(), overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(druidDataSource, coordinator, broker);

    cluster.callApi().verifySqlQuery(
        verifyQueryTemplate,
        druidDataSource,
        expectedCsv
    );
  }

  private void dropTableSafely(final String tableName)
  {
    try {
      icebergCatalog.dropTable(ICEBERG_NAMESPACE, tableName);
    }
    catch (Exception e) {
      // best-effort cleanup
    }
  }
}
