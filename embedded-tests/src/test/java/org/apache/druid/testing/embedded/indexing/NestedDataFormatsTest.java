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

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.TestHelper;
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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Embedded tests for nested data, ingested in different {@link NestedCommonFormatColumnFormatSpec}.
 */
public class NestedDataFormatsTest extends EmbeddedClusterTestBase
{
  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();

  private final String defaultFormat = "koala_default";

  @Override
  protected String getDatasourcePrefix()
  {
    return TestDataSource.KOALA;
  }

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addServer(overlord)
                               .addServer(coordinator)
                               .addServer(new EmbeddedIndexer())
                               .addServer(new EmbeddedHistorical())
                               .addServer(broker)
                               .addServer(new EmbeddedRouter());
  }

  @Override
  @BeforeAll
  protected void setup() throws Exception
  {
    super.setup();
    super.refreshDatasourceName();
    final TaskBuilder.IndexParallel indexTask =
        TaskBuilder.ofTypeIndexParallel()
                   .dataSource(defaultFormat)
                   .timestampColumn("timestamp")
                   .jsonInputFormat()
                   .inputSource(Resources.HttpData.kttm1Day())
                   .schemaDiscovery();

    final String taskId = EmbeddedClusterApis.newTaskId(defaultFormat);
    cluster.callApi().runTask(indexTask.withId(taskId), overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(defaultFormat, coordinator, broker);
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
                   .inputSource(Resources.HttpData.kttm1Day())
                   .schemaDiscovery()
                   .tuningConfig(t -> t.withIndexSpec(IndexSpec.builder().withAutoColumnFormatSpec(spec).build()));
    final String taskId = EmbeddedClusterApis.newTaskId(dataSource);
    cluster.callApi().runTask(indexTask.withId(taskId), overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    final String resultAsJson =
        cluster.callApi().onAnyBroker(b -> b.submitSqlQuery(ClientSqlQuery.simple("select * from sys.segments")));
    List<Map<String, Object>> result = JacksonUtils.readValue(
        TestHelper.JSON_MAPPER,
        resultAsJson.getBytes(StandardCharsets.UTF_8),
        new TypeReference<>()
        {
        }
    );
    Map<String, Object> defaultFormatResult =
        result.stream().filter(map -> defaultFormat.equals(map.get("datasource"))).findFirst().get();
    Map<String, Object> noneObjectStorageFormatResult =
        result.stream().filter(map -> dataSource.equals(map.get("datasource"))).findFirst().get();
    // Test ingesting with skipping raw json smile format works, same row count, with ~20% storage saving
    Assertions.assertEquals(465_346, defaultFormatResult.get("num_rows"));
    Assertions.assertEquals(53_000_804, defaultFormatResult.get("size"));
    Assertions.assertEquals(465_346, noneObjectStorageFormatResult.get("num_rows"));
    Assertions.assertEquals(41_938_750, noneObjectStorageFormatResult.get("size"));

    // Test querying on a nested field works
    final String groupByQuery = "select json_value(event, '$.type') as event_type, count(*) as total from %s group by 1 order by 2 desc, 1 asc limit 10";
    final String queryResultDefaultFormat = cluster.callApi().runSql(groupByQuery, defaultFormat);
    final String queryResultNoneObjectStorage = cluster.callApi().runSql(groupByQuery, dataSource);
    Assertions.assertEquals(queryResultDefaultFormat, queryResultNoneObjectStorage);

    // Test reconstruct json column works, the ordering of the fields has changed, but all values are perserved.
    final String scanQuery = "select event, to_json_string(agent) as agent from %s where json_value(event, '$.type') = 'PercentClear' and json_value(agent, '$.os') = 'Android' order by __time asc limit 1";
    final String scanQueryResultDefaultFormat = cluster.callApi().runSql(scanQuery, defaultFormat);
    final String scanQueryResultNoneObjectStorage = cluster.callApi().runSql(scanQuery, dataSource);
    // CHECKSTYLE: text blocks not supported in current Checkstyle version
    Assertions.assertEquals(
         """
         "{""type"":""PercentClear"",""percentage"":85}","{""type"":""Mobile Browser"",""category"":""Smartphone"",""browser"":""Chrome Mobile"",""browser_version"":""50.0.2661.89"",""os"":""Android"",""platform"":""Android""}"
         """.trim(), scanQueryResultDefaultFormat);
    Assertions.assertEquals(
         """
         "{""percentage"":85,""type"":""PercentClear""}","{""browser"":""Chrome Mobile"",""browser_version"":""50.0.2661.89"",""category"":""Smartphone"",""os"":""Android"",""platform"":""Android"",""type"":""Mobile Browser""}"
         """.trim(), scanQueryResultNoneObjectStorage);
  }
}
