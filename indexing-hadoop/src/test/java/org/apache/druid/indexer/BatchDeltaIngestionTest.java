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

package org.apache.druid.indexer;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.commons.io.FileUtils;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.indexer.hadoop.WindowedDataSegment;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.loading.LocalDataSegmentPuller;
import org.apache.druid.segment.realtime.firehose.IngestSegmentFirehose;
import org.apache.druid.segment.realtime.firehose.WindowedStorageAdapter;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.DataSegment.PruneSpecsHolder;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
        .addValue(PruneSpecsHolder.class, PruneSpecsHolder.DEFAULT);
    MAPPER.setInjectableValues(inject);
    INDEX_IO = HadoopDruidIndexerConfig.INDEX_IO;

    try {
      SEGMENT = MAPPER
          .readValue(
              BatchDeltaIngestionTest.class.getClassLoader().getResource("test-segment/descriptor.json"),
              DataSegment.class
          )
          .withLoadSpec(
              ImmutableMap.of(
                  "type",
                  "local",
                  "path",
                  BatchDeltaIngestionTest.class.getClassLoader().getResource("test-segment/index.zip").getPath()
              )
          );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testReindexing() throws Exception
  {
    List<WindowedDataSegment> segments = ImmutableList.of(new WindowedDataSegment(SEGMENT, INTERVAL_FULL));

    HadoopDruidIndexerConfig config = makeHadoopDruidIndexerConfig(
        ImmutableMap.of(
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
        ImmutableMap.of(
            "time", DateTimes.of("2014-10-22T00:00:00.000Z"),
            "host", ImmutableList.of("a.example.com"),
            "visited_sum", 100L,
            "unique_hosts", 1.0d
        ),
        ImmutableMap.of(
            "time", DateTimes.of("2014-10-22T01:00:00.000Z"),
            "host", ImmutableList.of("b.example.com"),
            "visited_sum", 150L,
            "unique_hosts", 1.0d
        ),
        ImmutableMap.of(
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
   * See https://github.com/apache/incubator-druid/issues/5277 .
   */
  @Test
  public void testReindexingWithNewAggregators() throws Exception
  {
    List<WindowedDataSegment> segments = ImmutableList.of(new WindowedDataSegment(SEGMENT, INTERVAL_FULL));

    AggregatorFactory[] aggregators = new AggregatorFactory[]{
        new LongSumAggregatorFactory("visited_sum2", "visited_sum"),
        new HyperUniquesAggregatorFactory("unique_hosts2", "unique_hosts")
    };

    Map<String, Object> inputSpec = ImmutableMap.of(
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
        ImmutableMap.of(
            "time", DateTimes.of("2014-10-22T00:00:00.000Z"),
            "host", ImmutableList.of("a.example.com"),
            "visited_sum2", 100L,
            "unique_hosts2", 1.0d
        ),
        ImmutableMap.of(
            "time", DateTimes.of("2014-10-22T01:00:00.000Z"),
            "host", ImmutableList.of("b.example.com"),
            "visited_sum2", 150L,
            "unique_hosts2", 1.0d
        ),
        ImmutableMap.of(
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
        ImmutableMap.of(
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
        ImmutableMap.of(
            "time", DateTimes.of("2014-10-22T00:00:00.000Z"),
            "host", ImmutableList.of("a.example.com"),
            "visited_sum", 100L,
            "unique_hosts", 1.0d
        ),
        ImmutableMap.of(
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
        ImmutableMap.of(
            "type",
            "multi",
            "children",
            ImmutableList.of(
                ImmutableMap.of(
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
        ImmutableMap.of(
            "time", DateTimes.of("2014-10-22T00:00:00.000Z"),
            "host", ImmutableList.of("a.example.com"),
            "visited_sum", 190L,
            "unique_hosts", 1.0d
        ),
        ImmutableMap.of(
            "time", DateTimes.of("2014-10-22T01:00:00.000Z"),
            "host", ImmutableList.of("b.example.com"),
            "visited_sum", 175L,
            "unique_hosts", 1.0d
        ),
        ImmutableMap.of(
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
    Assert.assertTrue(JobHelper.runJobs(ImmutableList.of(job), config));

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

    File indexZip = new File(segmentFolder, "index.zip");
    Assert.assertTrue(indexZip.exists());

    File tmpUnzippedSegmentDir = temporaryFolder.newFolder();
    new LocalDataSegmentPuller().getSegmentFiles(indexZip, tmpUnzippedSegmentDir);

    QueryableIndex index = INDEX_IO.loadIndex(tmpUnzippedSegmentDir);
    StorageAdapter adapter = new QueryableIndexStorageAdapter(index);

    Firehose firehose = new IngestSegmentFirehose(
        ImmutableList.of(new WindowedStorageAdapter(adapter, windowedDataSegment.getInterval())),
        TransformSpec.NONE,
        expectedDimensions,
        expectedMetrics,
        null
    );

    List<InputRow> rows = new ArrayList<>();
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
                new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, ImmutableList.of(INTERVAL_FULL)),
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
                null,
                null
            )
        )
    );

    config.setShardSpecs(
        ImmutableMap.of(
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
