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

package org.apache.druid.testing.embedded.compact;

import org.apache.druid.catalog.guice.CatalogClientModule;
import org.apache.druid.catalog.guice.CatalogCoordinatorModule;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.model.table.IndexingTemplateDefn;
import org.apache.druid.catalog.sync.CatalogClient;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.compact.CascadingCompactionTemplate;
import org.apache.druid.indexing.compact.CatalogCompactionJobTemplate;
import org.apache.druid.indexing.compact.CompactionJobTemplate;
import org.apache.druid.indexing.compact.CompactionRule;
import org.apache.druid.indexing.compact.CompactionStateMatcher;
import org.apache.druid.indexing.compact.CompactionSupervisorSpec;
import org.apache.druid.indexing.compact.InlineCompactionJobTemplate;
import org.apache.druid.indexing.compact.MSQCompactionJobTemplate;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.msq.guice.IndexerMemoryManagementModule;
import org.apache.druid.msq.guice.MSQDurableStorageModule;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.guice.MSQSqlModule;
import org.apache.druid.msq.guice.SqlTaskModule;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.rpc.UpdateResponse;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class CompactionSupervisorTest extends EmbeddedClusterTestBase
{
  protected final EmbeddedBroker broker = new EmbeddedBroker();
  protected final EmbeddedIndexer indexer = new EmbeddedIndexer()
      .setServerMemory(4_000_000_000L)
      .addProperty("druid.worker.capacity", "8");
  protected final EmbeddedOverlord overlord = new EmbeddedOverlord()
      .addProperty("druid.manager.segments.pollDuration", "PT1s")
      .addProperty("druid.manager.segments.useIncrementalCache", "always");
  protected final EmbeddedHistorical historical = new EmbeddedHistorical();
  protected final EmbeddedCoordinator coordinator = new EmbeddedCoordinator()
      .addProperty("druid.manager.segments.useIncrementalCache", "always");

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addExtensions(
                                   CatalogClientModule.class,
                                   CatalogCoordinatorModule.class,
                                   IndexerMemoryManagementModule.class,
                                   MSQDurableStorageModule.class,
                                   MSQIndexingModule.class,
                                   MSQSqlModule.class,
                                   SqlTaskModule.class
                               )
                               .addServer(coordinator)
                               .addServer(overlord)
                               .addServer(indexer)
                               .addServer(historical)
                               .addServer(broker)
                               .addServer(new EmbeddedRouter());
  }

  @BeforeAll
  public void enableCompactionSupervisors()
  {
    final UpdateResponse updateResponse = cluster.callApi().onLeaderOverlord(
        o -> o.updateClusterCompactionConfig(new ClusterCompactionConfig(1.0, 10, null, true, null))
    );
    Assertions.assertTrue(updateResponse.isSuccess());
  }

  @Test
  public void test_ingestDayGranularity_andCompactToMonthGranularity()
  {
    // Ingest data at DAY granularity and verify
    runIngestionAtGranularity(
        "DAY",
        "2025-06-01T00:00:00.000Z,shirt,105"
        + "\n2025-06-02T00:00:00.000Z,trousers,210"
        + "\n2025-06-03T00:00:00.000Z,jeans,150"
    );
    verifyDayAndMonthSegments(3, 0);

    // Create a compaction config with MONTH granularity
    InlineSchemaDataSourceCompactionConfig compactionConfig =
        InlineSchemaDataSourceCompactionConfig
            .builder()
            .forDataSource(dataSource)
            .withSkipOffsetFromLatest(Period.seconds(0))
            .withGranularitySpec(
                new UserCompactionTaskGranularityConfig(Granularities.MONTH, null, null)
            )
            .build();

    runCompactionWithSpec(compactionConfig);
    waitForCompactionTasksToFinish("compact", 1);

    verifyDayAndMonthSegments(0, 1);
  }

  @Test
  public void test_ingestHourGranularity_andCompactToDayAndMonth_withInlineTemplates()
  {
    // Create a cascading template with DAY and MONTH granularity
    CascadingCompactionTemplate cascadingTemplate = new CascadingCompactionTemplate(
        dataSource,
        List.of(
            new CompactionRule(Period.days(1), new InlineCompactionJobTemplate(createMatcher(Granularities.DAY))),
            new CompactionRule(Period.days(50), new InlineCompactionJobTemplate(createMatcher(Granularities.MONTH)))
        )
    );

    ingestRecordsAtGranularity(1200, "HOUR");
    runCompactionWithSpec(cascadingTemplate);
    waitForCompactionTasksToFinish("compact", 3);
    verifyDayAndMonthSegments(1, 2);
  }

  @Test
  public void test_ingestHourGranularity_andCompactToDayAndMonth_withCatalogTemplates()
  {
    ingestRecordsAtGranularity(1200, "HOUR");

    // Add compaction templates to catalog
    final String dayGranularityTemplateId = saveTemplateToCatalog(
        new InlineCompactionJobTemplate(createMatcher(Granularities.DAY))
    );
    final String monthGranularityTemplateId = saveTemplateToCatalog(
        new InlineCompactionJobTemplate(createMatcher(Granularities.MONTH))
    );

    // Create a cascading template with DAY and MONTH granularity
    CascadingCompactionTemplate cascadingTemplate = new CascadingCompactionTemplate(
        dataSource,
        List.of(
            new CompactionRule(Period.days(1), new CatalogCompactionJobTemplate(dayGranularityTemplateId, null)),
            new CompactionRule(Period.days(50), new CatalogCompactionJobTemplate(monthGranularityTemplateId, null))
        )
    );

    runCompactionWithSpec(cascadingTemplate);
    waitForCompactionTasksToFinish("compact", 3);
    verifyDayAndMonthSegments(1, 2);
  }

  @Test
  public void test_ingestHourGranularity_andCompactToDayAndMonth_withCatalogMSQTemplates()
  {
    ingestRecordsAtGranularity(1200, "HOUR");

    // Add compaction templates to catalog
    final String sqlDayGranularity =
        "REPLACE INTO ${dataSource}"
        + " OVERWRITE WHERE __time >= TIMESTAMP '${startDate}' AND __time < TIMESTAMP '${endDate}'"
        + " SELECT * FROM ${dataSource}"
        + " WHERE __time BETWEEN '${startDate}' AND '${endDate}'"
        + " PARTITIONED BY DAY";
    final String dayGranularityTemplateId = saveTemplateToCatalog(
        new MSQCompactionJobTemplate(
            new ClientSqlQuery(sqlDayGranularity, null, false, false, false, null, null),
            createMatcher(Granularities.DAY)
        )
    );
    final String sqlMonthGranularity =
        "REPLACE INTO ${dataSource}"
        + " OVERWRITE WHERE __time >= TIMESTAMP '${startDate}' AND __time < TIMESTAMP '${endDate}'"
        + " SELECT * FROM ${dataSource}"
        + " WHERE __time >= TIMESTAMP '${startDate}' AND __time < TIMESTAMP '${endDate}'"
        + " PARTITIONED BY MONTH";
    final String monthGranularityTemplateId = saveTemplateToCatalog(
        new MSQCompactionJobTemplate(
            new ClientSqlQuery(sqlMonthGranularity, null, false, false, false, null, null),
            createMatcher(Granularities.MONTH)
        )
    );

    // Create a cascading template with DAY and MONTH granularity
    CascadingCompactionTemplate cascadingTemplate = new CascadingCompactionTemplate(
        dataSource,
        List.of(
            new CompactionRule(Period.days(1), new CatalogCompactionJobTemplate(dayGranularityTemplateId, null)),
            new CompactionRule(Period.days(50), new CatalogCompactionJobTemplate(monthGranularityTemplateId, null))
        )
    );

    runCompactionWithSpec(cascadingTemplate);
    waitForCompactionTasksToFinish("query_controller", 4);
    verifyDayAndMonthSegments(1, 2);
  }

  private void ingestRecordsAtGranularity(int numRecords, String granularityName)
  {
    // Ingest data at HOUR granularity and verify
    Granularity granularity = Granularity.fromString(granularityName);
    runIngestionAtGranularity(
        granularityName,
        createHourlyInlineDataCsv(DateTimes.nowUtc(), numRecords)
    );
    List<DataSegment> segments = List.copyOf(
        overlord.bindings()
                .segmentsMetadataStorage()
                .retrieveAllUsedSegments(dataSource, Segments.ONLY_VISIBLE)
    );
    Assertions.assertEquals(numRecords, segments.size());
    segments.forEach(
        segment -> Assertions.assertTrue(granularity.isAligned(segment.getInterval()))
    );
  }

  private void runCompactionWithSpec(DataSourceCompactionConfig config)
  {
    final CompactionSupervisorSpec compactionSupervisor
        = new CompactionSupervisorSpec(config, false, null);
    cluster.callApi().postSupervisor(compactionSupervisor);
  }

  private void waitForCompactionTasksToFinish(String taskType, int expectedCount)
  {
    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("task/run/time")
                      .hasDimension(DruidMetrics.TASK_TYPE, taskType)
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasCountAtLeast(expectedCount)
    );
  }

  private void verifyDayAndMonthSegments(int expectedDaySegments, int expectedMonthSegments)
  {
    // Verify that segments are now compacted to MONTH and DAY granularity
    List<DataSegment> segments = List.copyOf(
        overlord.bindings()
                .segmentsMetadataStorage()
                .retrieveAllUsedSegments(dataSource, Segments.ONLY_VISIBLE)
    );

    int numMonthSegments = 0;
    int numDaySegments = 0;

    for (DataSegment segment : segments) {
      if (Granularities.DAY.isAligned(segment.getInterval())) {
        ++numDaySegments;
      } else if (Granularities.MONTH.isAligned(segment.getInterval())) {
        ++numMonthSegments;
      }
    }

    Assertions.assertTrue(numDaySegments >= expectedDaySegments);
    Assertions.assertTrue(numMonthSegments >= expectedMonthSegments);
  }

  private String saveTemplateToCatalog(CompactionJobTemplate template)
  {
    final String templateId = IdUtils.getRandomId();
    final CatalogClient catalogClient = overlord.bindings().getInstance(CatalogClient.class);

    final TableId tableId = TableId.of(TableId.INDEXING_TEMPLATE_SCHEMA, templateId);
    catalogClient.createTable(
        tableId,
        new TableSpec(
            IndexingTemplateDefn.TYPE,
            Map.of(IndexingTemplateDefn.PROPERTY_PAYLOAD, template),
            null
        )
    );

    ResolvedTable table = catalogClient.resolveTable(tableId);
    Assertions.assertNotNull(table);

    return templateId;
  }

  private void runIngestionAtGranularity(
      String granularity,
      String inlineDataCsv
  )
  {
    final String taskId = IdUtils.getRandomId();
    final IndexTask task = createIndexTaskForInlineData(taskId, granularity, inlineDataCsv);

    cluster.callApi().runTask(task, overlord);
  }

  private String createHourlyInlineDataCsv(DateTime latestRecordTimestamp, int numRecords)
  {
    final StringBuilder builder = new StringBuilder();
    for (int i = 0; i < numRecords; ++i) {
      builder.append(latestRecordTimestamp.minusHours(i))
             .append(",").append("item_").append(IdUtils.getRandomId())
             .append(",").append(0)
             .append("\n");
    }

    return builder.toString();
  }

  private IndexTask createIndexTaskForInlineData(String taskId, String granularity, String inlineDataCsv)
  {
    return MoreResources.Task.BASIC_INDEX
        .get()
        .segmentGranularity(granularity)
        .inlineInputSourceWithData(inlineDataCsv)
        .dataSource(dataSource)
        .withId(taskId);
  }

  private static CompactionStateMatcher createMatcher(Granularity segmentGranularity)
  {
    return new CompactionStateMatcher(
        null,
        null,
        null,
        null,
        null,
        new UserCompactionTaskGranularityConfig(segmentGranularity, null, null),
        null
    );
  }
}
