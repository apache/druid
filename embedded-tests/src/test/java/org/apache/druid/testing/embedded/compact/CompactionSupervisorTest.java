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
import org.apache.druid.indexing.compact.CompactionSupervisorSpec;
import org.apache.druid.indexing.compact.InlineCompactionJobTemplate;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.DruidMetrics;
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
import java.util.Set;

public class CompactionSupervisorTest extends EmbeddedClusterTestBase
{
  protected final EmbeddedBroker broker = new EmbeddedBroker();
  protected final EmbeddedIndexer indexer = new EmbeddedIndexer()
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
                               .addExtensions(CatalogClientModule.class, CatalogCoordinatorModule.class)
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
    Set<DataSegment> segments = cluster.callApi().getVisibleUsedSegments(dataSource, overlord);
    Assertions.assertEquals(3, segments.size());
    segments.forEach(
        segment -> Assertions.assertTrue(Granularities.DAY.isAligned(segment.getInterval()))
    );

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

    // Verify that segments are now compacted to MONTH granularity
    segments = cluster.callApi().getVisibleUsedSegments(dataSource, overlord);
    Assertions.assertEquals(1, segments.size());
    Assertions.assertTrue(
        Granularities.MONTH.isAligned(segments.iterator().next().getInterval())
    );
  }

  @Test
  public void test_ingestHourGranularity_andCompactToDayAndMonth_withInlineTemplates()
  {
    // Create a cascading template with DAY and MONTH granularity
    CascadingCompactionTemplate cascadingTemplate = new CascadingCompactionTemplate(
        dataSource,
        List.of(
            new CompactionRule(Period.days(2), new InlineCompactionJobTemplate(null, Granularities.DAY)),
            new CompactionRule(Period.days(100), new InlineCompactionJobTemplate(null, Granularities.MONTH))
        )
    );

    final CompactionSupervisorSpec compactionSupervisor
        = new CompactionSupervisorSpec(cascadingTemplate, false, null);
    cluster.callApi().postSupervisor(compactionSupervisor);

    ingestRecordsAtGranularity(2400, "HOUR");
    runCompactionWithSpec(cascadingTemplate);
    verifyDayAndMonth();
  }

  @Test
  public void test_ingestHourGranularity_andCompactToDayAndMonth_withCatalogTemplates()
  {
    ingestRecordsAtGranularity(2400, "HOUR");

    // Add compaction templates to catalog
    final String dayGranularityTemplateId = saveTemplateToCatalog(
        new InlineCompactionJobTemplate(null, Granularities.DAY)
    );
    final String monthGranularityTemplateId = saveTemplateToCatalog(
        new InlineCompactionJobTemplate(null, Granularities.MONTH)
    );

    // Create a cascading template with DAY and MONTH granularity
    CascadingCompactionTemplate cascadingTemplate = new CascadingCompactionTemplate(
        dataSource,
        List.of(
            new CompactionRule(Period.days(2), new CatalogCompactionJobTemplate(dayGranularityTemplateId, null)),
            new CompactionRule(Period.days(100), new CatalogCompactionJobTemplate(monthGranularityTemplateId, null))
        )
    );

    runCompactionWithSpec(cascadingTemplate);
    verifyDayAndMonth();
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

    // Wait for compaction tasks to be submitted
    final int numCompactionTasks = overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("compact/task/count")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource)
                      .hasValueAtLeast(1L)
    ).getValue().intValue();

    // Wait for the submitted tasks to finish
    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("task/run/time")
                      .hasDimension(DruidMetrics.TASK_TYPE, "compact")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasCountAtLeast(numCompactionTasks)
    );

  }

  private void verifyDayAndMonth()
  {
    // Verify that segments are now compacted to MONTH and DAY granularity
    List<DataSegment> segments = List.copyOf(
        overlord.bindings()
                .segmentsMetadataStorage()
                .retrieveAllUsedSegments(dataSource, Segments.ONLY_VISIBLE)
    );
    Assertions.assertTrue(segments.size() < 2400);

    int numMonthSegments = 0;
    int numDaySegments = 0;
    int numHourSegments = 0;

    for (DataSegment segment : segments) {
      if (Granularities.HOUR.isAligned(segment.getInterval())) {
        ++numHourSegments;
      } else if (Granularities.DAY.isAligned(segment.getInterval())) {
        ++numDaySegments;
      } else if (Granularities.MONTH.isAligned(segment.getInterval())) {
        ++numMonthSegments;
      }
    }

    // Verify that atleast 2 days are fully compacted to DAY
    Assertions.assertTrue(numDaySegments >= 2);

    // Verify that atleast 2 months are fully compacted to MONTH
    Assertions.assertTrue(numMonthSegments >= 2);

    // Verify that number of uncompacted days is between 5 and 38
    Assertions.assertTrue(5 * 24 <= numHourSegments && numHourSegments <= 38 * 24);
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
}
