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

package org.apache.druid.testing.embedded.query;

import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.dart.guice.DartControllerMemoryManagementModule;
import org.apache.druid.msq.dart.guice.DartControllerModule;
import org.apache.druid.msq.dart.guice.DartWorkerMemoryManagementModule;
import org.apache.druid.msq.dart.guice.DartWorkerModule;
import org.apache.druid.msq.guice.IndexerMemoryManagementModule;
import org.apache.druid.msq.guice.MSQDurableStorageModule;
import org.apache.druid.msq.guice.MSQExternalDataSourceModule;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.guice.MSQSqlModule;
import org.apache.druid.msq.guice.SqlTaskModule;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.query.DruidProcessingConfigTest;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.testing.embedded.minio.MinIOStorageResource;
import org.apache.druid.testing.embedded.msq.EmbeddedDurableShuffleStorageTest;
import org.apache.druid.testing.embedded.msq.EmbeddedMSQApis;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.io.ByteStreams;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Virtual storage fabric mode tests for classic native JSON queries
 */
class QueryVirtualStorageFabricTest extends EmbeddedClusterTestBase
{
  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedRouter router = new EmbeddedRouter();
  private final MinIOStorageResource storageResource = new MinIOStorageResource();

  private EmbeddedMSQApis msqApis;

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    historical.addProperty("druid.segmentCache.isVirtualStorageFabric", "true")
              .addProperty("druid.segmentCache.minVirtualStorageFabricLoadThreads", String.valueOf(Runtime.getRuntime().availableProcessors()))
              .addProperty("druid.segmentCache.maxVirtualStorageFabricLoadThreads", String.valueOf(Runtime.getRuntime().availableProcessors()))
              .addBeforeStartHook(
                  (cluster, self) -> self.addProperty(
                      "druid.segmentCache.locations",
                      StringUtils.format(
                          "[{\"path\":\"%s\",\"maxSize\":\"%s\"}]",
                          cluster.getTestFolder().newFolder().getAbsolutePath(),
                          HumanReadableBytes.parse("1MiB")
                      )
                  )
              )
              .addProperty("druid.server.maxSize", String.valueOf(HumanReadableBytes.parse("100MiB")));

    coordinator.addProperty("druid.manager.segments.useIncrementalCache", "always");

    overlord.addProperty("druid.manager.segments.useIncrementalCache", "always")
            .addProperty("druid.manager.segments.pollDuration", "PT0.1s");

    indexer.setServerMemory(400_000_000)
           .addProperty("druid.worker.capacity", "4")
           .addProperty("druid.processing.numThreads", "3")
           .addProperty("druid.segment.handoff.pollDuration", "PT0.1s");

    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        .addExtensions(
            DartControllerModule.class,
            DartWorkerModule.class,
            DartControllerMemoryManagementModule.class,
            DartWorkerMemoryManagementModule.class,
            IndexerMemoryManagementModule.class,
            MSQDurableStorageModule.class,
            MSQIndexingModule.class,
            MSQSqlModule.class,
            SqlTaskModule.class,
            MSQExternalDataSourceModule.class
        )
        .addResource(storageResource)
        .addServer(coordinator)
        .addServer(overlord)
        .addServer(indexer)
        .addServer(historical)
        .addServer(broker)
        .addServer(router);
  }

  @BeforeAll
  void loadData() throws IOException
  {
    msqApis = new EmbeddedMSQApis(cluster, overlord);
    dataSource = createTestDatasourceName();
    loadWikiData();
  }

  @Override
  protected void beforeEachTest()
  {
    // don't change the datasource name for each run because we set things up before all tests
  }

  @Test
  void testQueryTooMuchData()
  {
    Throwable t = Assertions.assertThrows(
        RuntimeException.class,
        () -> runSql("select * from \"%s\"", dataSource)
    );
    Assertions.assertTrue(t.getMessage().contains("Unable to load segment"));
    Assertions.assertTrue(t.getMessage().contains("] on demand, ensure enough disk space has been allocated to load all segments involved in the query"));
  }

  @Test
  void testQueryPartials()
  {
    // at the time this test was written, we can divide the segments up into these intervals and fit the required
    // segments in the cache, this is kind of brittle, but not quite sure what better to do and still expect exact
    // results..
    // "2015-09-12T00:00:00Z/2025-09-12T08:00:00Z"
    // "2015-09-12T08:00:00Z/2025-09-12T14:00:00Z"
    // "2015-09-12T14:00:00Z/2025-09-12T19:00:00Z"
    // "2015-09-12T19:00:00Z/2025-09-13T00:00:00Z"
    final String[] queries = new String[]{
        "select count(*) from \"%s\" WHERE __time >= TIMESTAMP '2015-09-12 00:00:00' and __time < TIMESTAMP '2015-09-12 08:00:00'",
        "select count(*) from \"%s\" WHERE __time >= TIMESTAMP '2015-09-12 08:00:00' and __time < TIMESTAMP '2015-09-12 14:00:00'",
        "select count(*) from \"%s\" WHERE __time >= TIMESTAMP '2015-09-12 14:00:00' and __time < TIMESTAMP '2015-09-12 19:00:00'",
        "select count(*) from \"%s\" WHERE __time >= TIMESTAMP '2015-09-12 19:00:00' and __time < TIMESTAMP '2015-09-13 00:00:00'"
    };
    final long[] expectedResults = new long[] {
        9770,
        10524,
        10267,
        8683
    };

    Assertions.assertEquals(expectedResults[0], Long.parseLong(runSql(queries[0], dataSource)));
    Assertions.assertEquals(expectedResults[1], Long.parseLong(runSql(queries[1], dataSource)));
    Assertions.assertEquals(expectedResults[2], Long.parseLong(runSql(queries[2], dataSource)));
    Assertions.assertEquals(expectedResults[3], Long.parseLong(runSql(queries[3], dataSource)));

    for (int i = 0; i < 1000; i++) {
      int nextQuery = ThreadLocalRandom.current().nextInt(queries.length);
      Assertions.assertEquals(expectedResults[nextQuery], Long.parseLong(runSql(queries[nextQuery], dataSource)));
    }
  }

  private String createTestDatasourceName()
  {
    return "wiki-" + IdUtils.getRandomId();
  }

  private String runSql(String sql, Object... args)
  {
    return FutureUtils.getUnchecked(
        cluster.anyBroker().submitSqlQuery(
            new ClientSqlQuery(
                StringUtils.format(sql, args),
                ResultFormat.CSV.name(),
                false,
                false,
                false,
                null,
                null
            )
        ),
        true
    ).trim();
  }

  /**
   * Stolen from {@link EmbeddedDurableShuffleStorageTest#loadWikipediaTable()} but with hourly granularity and no
   * durable shuffle location
   */
  private void loadWikiData() throws IOException
  {
    final File tmpDir = cluster.getTestFolder().newFolder();
    final File wikiFile = new File(tmpDir, "wiki.gz");

    ByteStreams.copy(
        DruidProcessingConfigTest.class.getResourceAsStream("/wikipedia/wikiticker-2015-09-12-sampled.json.gz"),
        Files.newOutputStream(wikiFile.toPath())
    );
    final String sql = StringUtils.format(
        "SET waitUntilSegmentsLoad = TRUE;\n"
        + "REPLACE INTO \"%s\" OVERWRITE ALL\n"
        + "SELECT\n"
        + "  TIME_PARSE(\"time\") AS __time,\n"
        + "  channel,\n"
        + "  countryName,\n"
        + "  page,\n"
        + "  \"user\",\n"
        + "  added,\n"
        + "  deleted,\n"
        + "  delta\n"
        + "FROM TABLE(\n"
        + "    EXTERN(\n"
        + "      %s,\n"
        + "      '{\"type\":\"json\"}',\n"
        + "      '[{\"name\":\"isRobot\",\"type\":\"string\"},{\"name\":\"channel\",\"type\":\"string\"},{\"name\":\"time\",\"type\":\"string\"},{\"name\":\"flags\",\"type\":\"string\"},{\"name\":\"isUnpatrolled\",\"type\":\"string\"},{\"name\":\"page\",\"type\":\"string\"},{\"name\":\"diffUrl\",\"type\":\"string\"},{\"name\":\"added\",\"type\":\"long\"},{\"name\":\"comment\",\"type\":\"string\"},{\"name\":\"commentLength\",\"type\":\"long\"},{\"name\":\"isNew\",\"type\":\"string\"},{\"name\":\"isMinor\",\"type\":\"string\"},{\"name\":\"delta\",\"type\":\"long\"},{\"name\":\"isAnonymous\",\"type\":\"string\"},{\"name\":\"user\",\"type\":\"string\"},{\"name\":\"deltaBucket\",\"type\":\"long\"},{\"name\":\"deleted\",\"type\":\"long\"},{\"name\":\"namespace\",\"type\":\"string\"},{\"name\":\"cityName\",\"type\":\"string\"},{\"name\":\"countryName\",\"type\":\"string\"},{\"name\":\"regionIsoCode\",\"type\":\"string\"},{\"name\":\"metroCode\",\"type\":\"long\"},{\"name\":\"countryIsoCode\",\"type\":\"string\"},{\"name\":\"regionName\",\"type\":\"string\"}]'\n"
        + "    )\n"
        + "  )\n"
        + "PARTITIONED BY HOUR\n"
        + "CLUSTERED BY channel",
        dataSource,
        Calcites.escapeStringLiteral(
            broker.bindings()
                  .jsonMapper()
                  .writeValueAsString(new LocalInputSource(null, null, Collections.singletonList(wikiFile), null))
        )
    );

    final MSQTaskReportPayload payload = msqApis.runTaskSqlAndGetReport(sql);
    Assertions.assertEquals(TaskState.SUCCESS, payload.getStatus().getStatus());
    Assertions.assertEquals(24, payload.getStatus().getSegmentLoadWaiterStatus().getTotalSegments());
    Assertions.assertNull(payload.getStatus().getErrorReport());
  }
}
