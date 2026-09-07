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

package org.apache.druid.testing.embedded.indexing;

import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.nested.NestedCommonFormatColumnFormatSpec;
import org.apache.druid.segment.nested.ObjectStorageEncoding;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Embedded tests for nested data, ingested in different {@link NestedCommonFormatColumnFormatSpec}.
 */
public class NestedDataFormatsTest extends EmbeddedClusterTestBase
{
  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();

  private final String datasourceWithDefaultFormat = EmbeddedClusterApis.createTestDatasourceName();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .useDefaultTimeoutForLatchableEmitter(120)
                               .addServer(overlord)
                               .addServer(coordinator)
                               .addServer(new EmbeddedIndexer())
                               .addServer(new EmbeddedHistorical())
                               .addServer(broker)
                               .addServer(new EmbeddedRouter());
  }

  @BeforeAll
  protected void ingestWithDefaultFormat()
  {
    final TaskBuilder.IndexParallel indexTask =
        TaskBuilder.ofTypeIndexParallel()
                   .dataSource(datasourceWithDefaultFormat)
                   .timestampColumn("timestamp")
                   .jsonInputFormat()
                   .inputSource(Resources.HttpData.kttmNested1Day())
                   .schemaDiscovery();

    final String taskId = EmbeddedClusterApis.newTaskId(datasourceWithDefaultFormat);
    cluster.callApi().runTask(indexTask.withId(taskId), overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(datasourceWithDefaultFormat, coordinator, broker);
  }

  @Test
  public void test_objectStorageEncoding()
  {
    // Ingest kttm data with skipping smile raw json format, comparing diff with defaultFormat
    NestedCommonFormatColumnFormatSpec spec =
        NestedCommonFormatColumnFormatSpec.builder().setObjectStorageEncoding(ObjectStorageEncoding.NONE).build();
    final TaskBuilder.IndexParallel indexTask =
        TaskBuilder.ofTypeIndexParallel()
                   .dataSource(dataSource)
                   .timestampColumn("timestamp")
                   .jsonInputFormat()
                   .inputSource(Resources.HttpData.kttmNested1Day())
                   .schemaDiscovery()
                   .tuningConfig(t -> t.withIndexSpec(IndexSpec.builder().withAutoColumnFormatSpec(spec).build()));
    final String taskId = EmbeddedClusterApis.newTaskId(dataSource);
    cluster.callApi().runTask(indexTask.withId(taskId), overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    // Test ingesting with skipping raw json smile format works, with the same row count
    final String rowsSql = "select sum(num_rows) from sys.segments where datasource = '%s'";
    final String defaultFormatRows = cluster.runSql(rowsSql, datasourceWithDefaultFormat);
    final String noneObjectStorageFormatRows = cluster.runSql(rowsSql, dataSource);
    Assertions.assertEquals(String.valueOf(465_346), defaultFormatRows);
    Assertions.assertEquals(String.valueOf(465_346), noneObjectStorageFormatRows);

    // Test ingesting with skipping raw json smile format works, with ~20% storage saving
    final String metadataSql = "select sum(size) from sys.segments where datasource = '%s'";
    final String defaultFormatSize = cluster.runSql(metadataSql, datasourceWithDefaultFormat);
    final String noneObjectStorageFormatSize = cluster.runSql(metadataSql, dataSource);
    System.out.println(noneObjectStorageFormatSize);
    System.out.println(defaultFormatSize);
    Assertions.assertTrue(
        Integer.parseInt(noneObjectStorageFormatSize) <= Integer.parseInt(defaultFormatSize) * 0.8,
        "Expected at least 20% space savings"
    );

    // Test querying on a nested field works
    final String groupByQuery =
        """
            select json_value(event, '$.type') as event_type, count(*) as total from %s
            group by 1 order by 2 desc, 1 asc limit 10
            """;
    final String queryResultDefaultFormat = cluster.runSql(groupByQuery, datasourceWithDefaultFormat);
    final String queryResultNoneObjectStorage = cluster.runSql(groupByQuery, dataSource);
    Assertions.assertEquals(queryResultDefaultFormat, queryResultNoneObjectStorage);

    // Test reconstruct json column works, the ordering of the fields has changed, but all values are perserved.
    final String scanQuery =
        """
            select event, to_json_string(agent) as agent from %s
            where json_value(event, '$.type') = 'PercentClear' and json_value(agent, '$.os') = 'Android'
            order by __time asc limit 1
            """;
    final String scanQueryResultDefaultFormat = cluster.runSql(scanQuery, datasourceWithDefaultFormat);
    final String scanQueryResultNoneObjectStorage = cluster.runSql(scanQuery, dataSource);
    // CHECKSTYLE: text blocks not supported in current Checkstyle version
    Assertions.assertEquals(
        """
            "{""type"":""PercentClear"",""percentage"":85}","{""type"":""Mobile Browser"",""category"":""Smartphone"",""browser"":""Chrome Mobile"",""browser_version"":""50.0.2661.89"",""os"":""Android"",""platform"":""Android""}"
            """.trim(),
        scanQueryResultDefaultFormat
    );
    Assertions.assertEquals(
        """
            "{""percentage"":85,""type"":""PercentClear""}","{""browser"":""Chrome Mobile"",""browser_version"":""50.0.2661.89"",""category"":""Smartphone"",""os"":""Android"",""platform"":""Android"",""type"":""Mobile Browser""}"
            """.trim(),
        scanQueryResultNoneObjectStorage
    );
  }
}
