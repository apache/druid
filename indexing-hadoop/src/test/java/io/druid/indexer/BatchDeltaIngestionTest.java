/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexer;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.data.input.Firehose;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.CSVParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.hll.HyperLogLogCollector;
import io.druid.indexer.hadoop.WindowedDataSegment;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.segment.IndexIO;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexStorageAdapter;
import io.druid.segment.StorageAdapter;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.loading.LocalDataSegmentPuller;
import io.druid.segment.realtime.firehose.IngestSegmentFirehose;
import io.druid.segment.realtime.firehose.WindowedStorageAdapter;
import io.druid.segment.transform.TransformSpec;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.commons.io.FileUtils;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class BatchDeltaIngestionTest
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final ObjectMapper MAPPER;
  private static final IndexIO INDEX_IO;
  private static final Interval INTERVAL_FULL = Intervals.of("2014-10-22T00:00:00Z/P1D");
  private static final Interval INTERVAL_PARTIAL = Intervals.of("2014-10-22T00:00:00Z/PT2H");
  private static final DataSegment SEGMENT;

  static {
    MAPPER = new DefaultObjectMapper();
    MAPPER.registerSubtypes(new NamedType(HashBasedNumberedShardSpec.class, "hashed"));
    InjectableValues inject = new InjectableValues.Std()
        .addValue(ObjectMapper.class, MAPPER)
        .addValue(DataSegment.PruneLoadSpecHolder.class, DataSegment.PruneLoadSpecHolder.DEFAULT);
    MAPPER.setInjectableValues(inject);
    INDEX_IO = HadoopDruidIndexerConfig.INDEX_IO;

    try {
      SEGMENT = MAPPER
          .readValue(
              BatchDeltaIngestionTest.class.getClassLoader().getResource("test-segment/descriptor.json"),
              DataSegment.class
          )
          .withLoadSpec(
              ImmutableMap.<String, Object>of(
                  "type",
                  "local",
                  "path",
                  BatchDeltaIngestionTest.class.getClassLoader().getResource("test-segment/index.zip").getPath()
              )
          );
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Test
  public void testReindexing() throws Exception
  {
    List<WindowedDataSegment> segments = ImmutableList.of(new WindowedDataSegment(SEGMENT, INTERVAL_FULL));

    HadoopDruidIndexerConfig config = makeHadoopDruidIndexerConfig(
        ImmutableMap.<String, Object>of(
            "type",
            "dataSource",
            "ingestionSpec",
            ImmutableMap.of(
                "dataSource",
                "testds",
                "interval",
                INTERVAL_FULL
            ),
            "segments",
            segments
        ),
        temporaryFolder.newFolder()
    );

    List<ImmutableMap<String, Object>> expectedRows = ImmutableList.of(
        ImmutableMap.<String, Object>of(
            "time", DateTimes.of("2014-10-22T00:00:00.000Z"),
            "host", ImmutableList.of("a.example.com"),
            "visited_sum", 100L,
            "unique_hosts", 1.0d
        ),
        ImmutableMap.<String, Object>of(
            "time", DateTimes.of("2014-10-22T01:00:00.000Z"),
            "host", ImmutableList.of("b.example.com"),
            "visited_sum", 150L,
            "unique_hosts", 1.0d
        ),
        ImmutableMap.<String, Object>of(
            "time", DateTimes.of("2014-10-22T02:00:00.000Z"),
            "host", ImmutableList.of("c.example.com"),
            "visited_sum", 200L,
            "unique_hosts", 1.0d
        )
    );

    testIngestion(
        config,
        expectedRows,
        Iterables.getOnlyElement(segments),
        ImmutableList.of("host"),
        ImmutableList.of("visited_sum", "unique_hosts")
    );
  }

  /**
   * By default re-indexing expects same aggregators as used by original indexing job. But, with additional flag
   * "useNewAggs" in DatasourcePathSpec, user can optionally have any set of aggregators.
   * See https://github.com/druid-io/druid/issues/5277 .
   */
  @Test
  public void testReindexingWithNewAggregators() throws Exception
  {
    List<WindowedDataSegment> segments = ImmutableList.of(new WindowedDataSegment(SEGMENT, INTERVAL_FULL));

    AggregatorFactory[] aggregators = new AggregatorFactory[]{
        new LongSumAggregatorFactory("visited_sum2", "visited_sum"),
        new HyperUniquesAggregatorFactory("unique_hosts2", "unique_hosts")
    };

    Map<String, Object> inputSpec = ImmutableMap.<String, Object>of(
        "type",
        "dataSource",
        "ingestionSpec",
        ImmutableMap.of(
            "dataSource",
            "testds",
            "interval",
            INTERVAL_FULL
        ),
        "segments",
        segments,
        "useNewAggs", true
    );

    File tmpDir = temporaryFolder.newFolder();

    HadoopDruidIndexerConfig config = makeHadoopDruidIndexerConfig(
        inputSpec,
        tmpDir,
        aggregators
    );

    List<ImmutableMap<String, Object>> expectedRows = ImmutableList.of(
        ImmutableMap.<String, Object>of(
            "time", DateTimes.of("2014-10-22T00:00:00.000Z"),
            "host", ImmutableList.of("a.example.com"),
            "visited_sum2", 100L,
            "unique_hosts2", 1.0d
        ),
        ImmutableMap.<String, Object>of(
            "time", DateTimes.of("2014-10-22T01:00:00.000Z"),
            "host", ImmutableList.of("b.example.com"),
            "visited_sum2", 150L,
            "unique_hosts2", 1.0d
        ),
        ImmutableMap.<String, Object>of(
            "time", DateTimes.of("2014-10-22T02:00:00.000Z"),
            "host", ImmutableList.of("c.example.com"),
            "visited_sum2", 200L,
            "unique_hosts2", 1.0d
        )
    );

    testIngestion(
        config,
        expectedRows,
        Iterables.getOnlyElement(segments),
        ImmutableList.of("host"),
        ImmutableList.of("visited_sum2", "unique_hosts2")
    );
  }

  @Test
  public void testReindexingWithPartialWindow() throws Exception
  {
    List<WindowedDataSegment> segments = ImmutableList.of(new WindowedDataSegment(SEGMENT, INTERVAL_PARTIAL));

    HadoopDruidIndexerConfig config = makeHadoopDruidIndexerConfig(
        ImmutableMap.<String, Object>of(
            "type",
            "dataSource",
            "ingestionSpec",
            ImmutableMap.of(
                "dataSource",
                "testds",
                "interval",
                INTERVAL_FULL
            ),
            "segments",
            segments
        ),
        temporaryFolder.newFolder()
    );

    List<ImmutableMap<String, Object>> expectedRows = ImmutableList.of(
        ImmutableMap.<String, Object>of(
            "time", DateTimes.of("2014-10-22T00:00:00.000Z"),
            "host", ImmutableList.of("a.example.com"),
            "visited_sum", 100L,
            "unique_hosts", 1.0d
        ),
        ImmutableMap.<String, Object>of(
            "time", DateTimes.of("2014-10-22T01:00:00.000Z"),
            "host", ImmutableList.of("b.example.com"),
            "visited_sum", 150L,
            "unique_hosts", 1.0d
        )
    );

    testIngestion(
        config,
        expectedRows,
        Iterables.getOnlyElement(segments),
        ImmutableList.of("host"),
        ImmutableList.of("visited_sum", "unique_hosts")
    );
  }

  @Test
  public void testDeltaIngestion() throws Exception
  {
    File tmpDir = temporaryFolder.newFolder();

    File dataFile1 = new File(tmpDir, "data1");
    FileUtils.writeLines(
        dataFile1,
        ImmutableList.of(
            "2014102200,a.example.com,a.example.com,90",
            "2014102201,b.example.com,b.example.com,25"
        )
    );

    File dataFile2 = new File(tmpDir, "data2");
    FileUtils.writeLines(
        dataFile2,
        ImmutableList.of(
            "2014102202,c.example.com,c.example.com,70"
        )
    );

    //using a hadoop glob path to test that it continues to work with hadoop MultipleInputs usage and not
    //affected by
    //https://issues.apache.org/jira/browse/MAPREDUCE-5061
    String inputPath = tmpDir.getPath() + "/{data1,data2}";

    List<WindowedDataSegment> segments = ImmutableList.of(new WindowedDataSegment(SEGMENT, INTERVAL_FULL));

    HadoopDruidIndexerConfig config = makeHadoopDruidIndexerConfig(
        ImmutableMap.<String, Object>of(
            "type",
            "multi",
            "children",
            ImmutableList.of(
                ImmutableMap.<String, Object>of(
                    "type",
                    "dataSource",
                    "ingestionSpec",
                    ImmutableMap.of(
                        "dataSource",
                        "testds",
                        "interval",
                        INTERVAL_FULL
                    ),
                    "segments",
                    segments
                ),
                ImmutableMap.<String, Object>of(
                    "type",
                    "static",
                    "paths",
                    inputPath
                )
            )
        ),
        temporaryFolder.newFolder()
    );

    List<ImmutableMap<String, Object>> expectedRows = ImmutableList.of(
        ImmutableMap.<String, Object>of(
            "time", DateTimes.of("2014-10-22T00:00:00.000Z"),
            "host", ImmutableList.of("a.example.com"),
            "visited_sum", 190L,
            "unique_hosts", 1.0d
        ),
        ImmutableMap.<String, Object>of(
            "time", DateTimes.of("2014-10-22T01:00:00.000Z"),
            "host", ImmutableList.of("b.example.com"),
            "visited_sum", 175L,
            "unique_hosts", 1.0d
        ),
        ImmutableMap.<String, Object>of(
            "time", DateTimes.of("2014-10-22T02:00:00.000Z"),
            "host", ImmutableList.of("c.example.com"),
            "visited_sum", 270L,
            "unique_hosts", 1.0d
        )
    );

    testIngestion(
        config,
        expectedRows,
        Iterables.getOnlyElement(segments),
        ImmutableList.of("host"),
        ImmutableList.of("visited_sum", "unique_hosts")
    );
  }

  private void testIngestion(
      HadoopDruidIndexerConfig config,
      List<ImmutableMap<String, Object>> expectedRowsGenerated,
      WindowedDataSegment windowedDataSegment,
      List<String> expectedDimensions,
      List<String> expectedMetrics
  ) throws Exception
  {
    IndexGeneratorJob job = new IndexGeneratorJob(config);
    Assert.assertTrue(JobHelper.runJobs(ImmutableList.<Jobby>of(job), config));

    File segmentFolder = new File(
        StringUtils.format(
            "%s/%s/%s_%s/%s/0",
            config.getSchema().getIOConfig().getSegmentOutputPath(),
            config.getSchema().getDataSchema().getDataSource(),
            INTERVAL_FULL.getStart().toString(),
            INTERVAL_FULL.getEnd().toString(),
            config.getSchema().getTuningConfig().getVersion()
        )
    );

    Assert.assertTrue(segmentFolder.exists());

    File descriptor = new File(segmentFolder, "descriptor.json");
    File indexZip = new File(segmentFolder, "index.zip");
    Assert.assertTrue(descriptor.exists());
    Assert.assertTrue(indexZip.exists());

    DataSegment dataSegment = MAPPER.readValue(descriptor, DataSegment.class);
    Assert.assertEquals("website", dataSegment.getDataSource());
    Assert.assertEquals(config.getSchema().getTuningConfig().getVersion(), dataSegment.getVersion());
    Assert.assertEquals(INTERVAL_FULL, dataSegment.getInterval());
    Assert.assertEquals("local", dataSegment.getLoadSpec().get("type"));
    Assert.assertEquals(indexZip.getCanonicalPath(), dataSegment.getLoadSpec().get("path"));
    Assert.assertEquals(expectedDimensions, dataSegment.getDimensions());
    Assert.assertEquals(expectedMetrics, dataSegment.getMetrics());
    Assert.assertEquals(Integer.valueOf(9), dataSegment.getBinaryVersion());

    HashBasedNumberedShardSpec spec = (HashBasedNumberedShardSpec) dataSegment.getShardSpec();
    Assert.assertEquals(0, spec.getPartitionNum());
    Assert.assertEquals(1, spec.getPartitions());

    File tmpUnzippedSegmentDir = temporaryFolder.newFolder();
    new LocalDataSegmentPuller().getSegmentFiles(dataSegment, tmpUnzippedSegmentDir);

    QueryableIndex index = INDEX_IO.loadIndex(tmpUnzippedSegmentDir);
    StorageAdapter adapter = new QueryableIndexStorageAdapter(index);

    Firehose firehose = new IngestSegmentFirehose(
        ImmutableList.of(new WindowedStorageAdapter(adapter, windowedDataSegment.getInterval())),
        TransformSpec.NONE,
        expectedDimensions,
        expectedMetrics,
        null
    );

    List<InputRow> rows = Lists.newArrayList();
    while (firehose.hasMore()) {
      rows.add(firehose.nextRow());
    }

    verifyRows(expectedRowsGenerated, rows, expectedDimensions, expectedMetrics);
  }

  private HadoopDruidIndexerConfig makeHadoopDruidIndexerConfig(Map<String, Object> inputSpec, File tmpDir)
      throws Exception
  {
    return makeHadoopDruidIndexerConfig(inputSpec, tmpDir, null);
  }

  private HadoopDruidIndexerConfig makeHadoopDruidIndexerConfig(
      Map<String, Object> inputSpec,
      File tmpDir,
      AggregatorFactory[] aggregators
  )
      throws Exception
  {
    HadoopDruidIndexerConfig config = new HadoopDruidIndexerConfig(
        new HadoopIngestionSpec(
            new DataSchema(
                "website",
                MAPPER.convertValue(
                    new StringInputRowParser(
                        new CSVParseSpec(
                            new TimestampSpec("timestamp", "yyyyMMddHH", null),
                            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("host")), null, null),
                            null,
                            ImmutableList.of("timestamp", "host", "host2", "visited_num"),
                            false,
                            0
                        ),
                        null
                    ),
                    Map.class
                ),
                aggregators != null ? aggregators : new AggregatorFactory[]{
                    new LongSumAggregatorFactory("visited_sum", "visited_num"),
                    new HyperUniquesAggregatorFactory("unique_hosts", "host2")
                },
                new UniformGranularitySpec(
                    Granularities.DAY, Granularities.NONE, ImmutableList.of(INTERVAL_FULL)
                ),
                null,
                MAPPER
            ),
            new HadoopIOConfig(
                inputSpec,
                null,
                tmpDir.getCanonicalPath()
            ),
            new HadoopTuningConfig(
                tmpDir.getCanonicalPath(),
                null,
                null,
                null,
                null,
                null,
                false,
                false,
                false,
                false,
                null,
                false,
                false,
                null,
                null,
                null,
                false,
                false,
                null,
                null,
                null
            )
        )
    );

    config.setShardSpecs(
        ImmutableMap.<Long, List<HadoopyShardSpec>>of(
            INTERVAL_FULL.getStartMillis(),
            ImmutableList.of(
                new HadoopyShardSpec(
                    new HashBasedNumberedShardSpec(0, 1, null, HadoopDruidIndexerConfig.JSON_MAPPER),
                    0
                )
            )
        )
    );
    config = HadoopDruidIndexerConfig.fromSpec(config.getSchema());
    return config;
  }

  private void verifyRows(
      List<ImmutableMap<String, Object>> expectedRows,
      List<InputRow> actualRows,
      List<String> expectedDimensions,
      List<String> expectedMetrics
  )
  {
    Assert.assertEquals(expectedRows.size(), actualRows.size());

    for (int i = 0; i < expectedRows.size(); i++) {
      Map<String, Object> expected = expectedRows.get(i);
      InputRow actual = actualRows.get(i);

      Assert.assertEquals(expected.get("time"), actual.getTimestamp());

      Assert.assertEquals(expectedDimensions, actual.getDimensions());

      expectedDimensions.forEach(s -> Assert.assertEquals(expected.get(s), actual.getDimension(s)));

      for (String metric : expectedMetrics) {
        Object actualValue = actual.getRaw(metric);
        if (actualValue instanceof HyperLogLogCollector) {
          Assert.assertEquals(
              (Double) expected.get(metric),
              (Double) HyperUniquesAggregatorFactory.estimateCardinality(actualValue, false),
              0.001
          );
        } else {
          Assert.assertEquals(expected.get(metric), actual.getMetric(metric));
        }
      }
    }
  }
}
