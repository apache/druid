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

package org.apache.druid.testing.embedded.tsfile;

import org.apache.druid.data.input.tsfile.TsFileExtensionsModule;
import org.apache.druid.data.input.tsfile.TsFileInputFormat;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.testing.embedded.msq.EmbeddedMSQApis;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.ColumnSchemaBuilder;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.v4.TsFileWriterBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

/**
 * End-to-end ingestion tests for {@link TsFileInputFormat} running inside an embedded Druid cluster.
 * The TsFile written by {@link #createTsFile()} holds three rows of one table, mirroring the sample file
 * shipped with the extension.
 */
public class TsFileIngestionTest extends EmbeddedClusterTestBase
{
  private static final String TABLE_NAME = "sensor";
  private static final int ROW_COUNT = 3;

  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer()
      .setServerMemory(300_000_000L)
      .addProperty("druid.worker.capacity", "2");
  private final EmbeddedBroker broker = new EmbeddedBroker();

  @TempDir
  private File tempDir;

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        .addExtension(TsFileExtensionsModule.class)
        .addServer(overlord)
        .addServer(coordinator)
        .addServer(indexer)
        .addServer(broker)
        .addServer(new EmbeddedHistorical());
  }

  /**
   * Batch ingestion driven by a task spec that reads a local TsFile.
   */
  @Test
  @Timeout(180)
  public void testBatchIngestionTask() throws IOException, WriteProcessException
  {
    final IndexTask task = TaskBuilder
        .ofTypeIndex()
        .localInputSourceWithFiles(createTsFile())
        .inputFormat(new TsFileInputFormat(null, null, null))
        .timestampColumn(TsFileInputFormat.TIME_COLUMN_NAME)
        .dimensions("region", "device")
        .metricAggregates(new DoubleSumAggregatorFactory("temperature", "temperature"))
        .segmentGranularity("DAY")
        .dataSource(dataSource)
        .withId(EmbeddedClusterApis.newTaskId(dataSource));

    cluster.callApi().runTask(task, overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    assertIngestedRows();
  }

  /**
   * MSQ {@code INSERT ... SELECT FROM TABLE(EXTERN(...))} reading the same TsFile.
   */
  @Test
  @Timeout(180)
  public void testInsertSelectFromExtern() throws IOException, WriteProcessException
  {
    final File tsFile = createTsFile();
    final EmbeddedMSQApis msqApis = new EmbeddedMSQApis(cluster, overlord);

    // Escape backslashes so a Windows path stays valid inside the JSON input source spec.
    final String baseDir = StringUtils.replace(tsFile.getParentFile().getAbsolutePath(), "\\", "\\\\");

    final String sql = StringUtils.format(
        "INSERT INTO %s\n"
        + "SELECT\n"
        + "  MILLIS_TO_TIMESTAMP(\"Time\") AS __time,\n"
        + "  region,\n"
        + "  device,\n"
        + "  temperature\n"
        + "FROM TABLE(\n"
        + "  EXTERN(\n"
        + "    '{\"type\":\"local\",\"baseDir\":\"%s\",\"filter\":\"*.tsfile\"}',\n"
        + "    '{\"type\":\"tsfile\",\"tables\":[\"%s\"]}',\n"
        + "    '[{\"name\":\"Time\",\"type\":\"long\"},{\"name\":\"region\",\"type\":\"string\"},"
        + "{\"name\":\"device\",\"type\":\"string\"},{\"name\":\"temperature\",\"type\":\"double\"}]'\n"
        + "  )\n"
        + ")\n"
        + "PARTITIONED BY DAY",
        dataSource,
        baseDir,
        TABLE_NAME
    );

    final SqlTaskStatus taskStatus = msqApis.submitTaskSql(sql);
    cluster.callApi().waitForTaskToSucceed(taskStatus.getTaskId(), overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    assertIngestedRows();
    cluster.callApi().verifySqlQuery(
        "SELECT SUM(temperature) FROM %s",
        dataSource,
        String.valueOf(21.5 + 22.5 + 23.5)
    );
  }

  private void assertIngestedRows()
  {
    cluster.callApi().verifySqlQuery(
        "SELECT COUNT(*) FROM %s",
        dataSource,
        String.valueOf(ROW_COUNT)
    );
    cluster.callApi().verifySqlQuery(
        "SELECT region, COUNT(*) FROM %s GROUP BY region ORDER BY region",
        dataSource,
        "beijing,2\nshanghai,1"
    );
    cluster.callApi().verifySqlQuery(
        "SELECT MIN(__time), MAX(__time) FROM %s",
        dataSource,
        "1970-01-01T00:00:01.000Z,1970-01-01T00:00:01.002Z"
    );
  }

  private File createTsFile() throws IOException, WriteProcessException
  {
    final File file = new File(tempDir, "sensor.tsfile");
    final TableSchema tableSchema = new TableSchema(
        TABLE_NAME,
        Arrays.asList(
            new ColumnSchemaBuilder().name("region").dataType(TSDataType.STRING).category(ColumnCategory.TAG).build(),
            new ColumnSchemaBuilder().name("device").dataType(TSDataType.STRING).category(ColumnCategory.TAG).build(),
            new ColumnSchemaBuilder().name("temperature").dataType(TSDataType.FLOAT).build()
        )
    );

    final Tablet tablet = new Tablet(
        Arrays.asList("region", "device", "temperature"),
        Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.FLOAT)
    );
    for (int i = 0; i < ROW_COUNT; i++) {
      tablet.addTimestamp(i, 1000L + i);
      tablet.addValue(i, "region", i < 2 ? "beijing" : "shanghai");
      tablet.addValue(i, "device", "d" + (i + 1));
      tablet.addValue(i, "temperature", 21.5f + i);
    }

    try (ITsFileWriter writer = new TsFileWriterBuilder().file(file).tableSchema(tableSchema).build()) {
      writer.write(tablet);
    }
    return file;
  }
}
