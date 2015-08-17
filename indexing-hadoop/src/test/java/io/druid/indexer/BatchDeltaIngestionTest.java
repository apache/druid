/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexer;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.metamx.common.Granularity;
import io.druid.data.input.Firehose;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.CSVParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.indexer.path.DatasourcePathSpec;
import io.druid.indexer.path.UsedSegmentLister;
import io.druid.jackson.DefaultObjectMapper;
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
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
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
  public final
  @Rule
  TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ObjectMapper mapper;
  private Interval interval;
  private List<DataSegment> segments;

  public BatchDeltaIngestionTest() throws IOException
  {
    mapper = new DefaultObjectMapper();
    mapper.registerSubtypes(new NamedType(HashBasedNumberedShardSpec.class, "hashed"));
    InjectableValues inject = new InjectableValues.Std().addValue(ObjectMapper.class, mapper);
    mapper.setInjectableValues(inject);

    this.interval = new Interval("2014-10-22T00:00:00Z/P1D");
    segments = ImmutableList.of(
        new DefaultObjectMapper()
            .readValue(
                this.getClass().getClassLoader().getResource("test-segment/descriptor.json"),
                DataSegment.class
            )
            .withLoadSpec(
                ImmutableMap.<String, Object>of(
                    "type",
                    "local",
                    "path",
                    this.getClass().getClassLoader().getResource("test-segment/index.zip").getPath()
                )
            )
    );
  }

  @Test
  public void testReindexing() throws Exception
  {
    HadoopDruidIndexerConfig config = makeHadoopDruidIndexerConfig(
        ImmutableMap.<String, Object>of(
            "type",
            "dataSource",
            "ingestionSpec",
            ImmutableMap.of(
                "dataSource",
                "xyz",
                "interval",
                interval
            ),
            "segments",
            segments
        ),
        temporaryFolder.newFolder()
    );

    List<ImmutableMap<String, Object>> expectedRows = ImmutableList.of(
        ImmutableMap.<String, Object>of(
            "time", DateTime.parse("2014-10-22T00:00:00.000Z"),
            "host", ImmutableList.of("a.example.com"),
            "visited_sum", 100L,
            "unique_hosts", 1.0d
        ),
        ImmutableMap.<String, Object>of(
            "time", DateTime.parse("2014-10-22T01:00:00.000Z"),
            "host", ImmutableList.of("b.example.com"),
            "visited_sum", 150L,
            "unique_hosts", 1.0d
        ),
        ImmutableMap.<String, Object>of(
            "time", DateTime.parse("2014-10-22T02:00:00.000Z"),
            "host", ImmutableList.of("c.example.com"),
            "visited_sum", 200L,
            "unique_hosts", 1.0d
        )
    );

    testIngestion(config, expectedRows);
  }

  @Test
  public void testDeltaIngestion() throws Exception
  {
    File dataFile = temporaryFolder.newFile();
    FileUtils.writeLines(
        dataFile,
        ImmutableList.of(
            "2014102200,a.example.com,a.example.com,90",
            "2014102201,b.example.com,b.example.com,25",
            "2014102202,c.example.com,c.example.com,70"
        )
    );

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
                        "xyz",
                        "interval",
                        interval
                    ),
                    "segments",
                    segments
                ),
                ImmutableMap.<String, Object>of(
                    "type",
                    "static",
                    "paths",
                    dataFile.getCanonicalPath()
                )
            )
        ),
        temporaryFolder.newFolder()
    );

    List<ImmutableMap<String, Object>> expectedRows = ImmutableList.of(
        ImmutableMap.<String, Object>of(
            "time", DateTime.parse("2014-10-22T00:00:00.000Z"),
            "host", ImmutableList.of("a.example.com"),
            "visited_sum", 190L,
            "unique_hosts", 1.0d
        ),
        ImmutableMap.<String, Object>of(
            "time", DateTime.parse("2014-10-22T01:00:00.000Z"),
            "host", ImmutableList.of("b.example.com"),
            "visited_sum", 175L,
            "unique_hosts", 1.0d
        ),
        ImmutableMap.<String, Object>of(
            "time", DateTime.parse("2014-10-22T02:00:00.000Z"),
            "host", ImmutableList.of("c.example.com"),
            "visited_sum", 270L,
            "unique_hosts", 1.0d
        )
    );

    testIngestion(config, expectedRows);
  }

  private void testIngestion(HadoopDruidIndexerConfig config, List<ImmutableMap<String, Object>> expectedRowsGenerated)
      throws Exception
  {
    IndexGeneratorJob job = new LegacyIndexGeneratorJob(config);
    JobHelper.runJobs(ImmutableList.<Jobby>of(job), config);

    File segmentFolder = new File(
        String.format(
            "%s/%s/%s_%s/%s/0",
            config.getSchema().getIOConfig().getSegmentOutputPath(),
            config.getSchema().getDataSchema().getDataSource(),
            interval.getStart().toString(),
            interval.getEnd().toString(),
            config.getSchema().getTuningConfig().getVersion()
        )
    );

    Assert.assertTrue(segmentFolder.exists());

    File descriptor = new File(segmentFolder, "descriptor.json");
    File indexZip = new File(segmentFolder, "index.zip");
    Assert.assertTrue(descriptor.exists());
    Assert.assertTrue(indexZip.exists());

    DataSegment dataSegment = mapper.readValue(descriptor, DataSegment.class);
    Assert.assertEquals("website", dataSegment.getDataSource());
    Assert.assertEquals(config.getSchema().getTuningConfig().getVersion(), dataSegment.getVersion());
    Assert.assertEquals(interval, dataSegment.getInterval());
    Assert.assertEquals("local", dataSegment.getLoadSpec().get("type"));
    Assert.assertEquals(indexZip.getCanonicalPath(), dataSegment.getLoadSpec().get("path"));
    Assert.assertEquals("host", dataSegment.getDimensions().get(0));
    Assert.assertEquals("visited_sum", dataSegment.getMetrics().get(0));
    Assert.assertEquals("unique_hosts", dataSegment.getMetrics().get(1));
    Assert.assertEquals(Integer.valueOf(9), dataSegment.getBinaryVersion());

    HashBasedNumberedShardSpec spec = (HashBasedNumberedShardSpec) dataSegment.getShardSpec();
    Assert.assertEquals(0, spec.getPartitionNum());
    Assert.assertEquals(1, spec.getPartitions());

    File tmpUnzippedSegmentDir = temporaryFolder.newFolder();
    new LocalDataSegmentPuller().getSegmentFiles(dataSegment, tmpUnzippedSegmentDir);

    QueryableIndex index = IndexIO.loadIndex(tmpUnzippedSegmentDir);
    StorageAdapter adapter = new QueryableIndexStorageAdapter(index);

    Firehose firehose = new IngestSegmentFirehose(
        ImmutableList.of(adapter),
        ImmutableList.of("host"),
        ImmutableList.of("visited_sum", "unique_hosts"),
        null,
        interval,
        QueryGranularity.NONE
    );

    List<InputRow> rows = Lists.newArrayList();
    while (firehose.hasMore()) {
      rows.add(firehose.nextRow());
    }

    verifyRows(expectedRowsGenerated, rows);
  }

  private HadoopDruidIndexerConfig makeHadoopDruidIndexerConfig(Map<String, Object> inputSpec, File tmpDir)
      throws Exception
  {
    HadoopDruidIndexerConfig config = new HadoopDruidIndexerConfig(
        new HadoopIngestionSpec(
            new DataSchema(
                "website",
                new StringInputRowParser(
                    new CSVParseSpec(
                        new TimestampSpec("timestamp", "yyyyMMddHH", null),
                        new DimensionsSpec(ImmutableList.of("host"), null, null),
                        null,
                        ImmutableList.of("timestamp", "host", "host2", "visited_num")
                    )
                ),
                new AggregatorFactory[]{
                    new LongSumAggregatorFactory("visited_sum", "visited_num"),
                    new HyperUniquesAggregatorFactory("unique_hosts", "host2")
                },
                new UniformGranularitySpec(
                    Granularity.DAY, QueryGranularity.NONE, ImmutableList.of(this.interval)
                )
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
                false,
                null,
                null,
                false
            )
        )
    );

    config.setShardSpecs(
        ImmutableMap.<DateTime, List<HadoopyShardSpec>>of(
            interval.getStart(),
            ImmutableList.of(
                new HadoopyShardSpec(
                    new HashBasedNumberedShardSpec(0, 1, HadoopDruidIndexerConfig.jsonMapper),
                    0
                )
            )
        )
    );
    config = HadoopDruidIndexerConfig.fromSpec(config.getSchema());
    return config;
  }

  private void verifyRows(List<ImmutableMap<String, Object>> expectedRows, List<InputRow> actualRows)
  {
    Assert.assertEquals(expectedRows.size(), actualRows.size());

    for (int i = 0; i < expectedRows.size(); i++) {
      Map<String, Object> expected = expectedRows.get(i);
      InputRow actual = actualRows.get(i);

      Assert.assertEquals(ImmutableList.of("host"), actual.getDimensions());

      Assert.assertEquals(expected.get("time"), actual.getTimestamp());
      Assert.assertEquals(expected.get("host"), actual.getDimension("host"));
      Assert.assertEquals(expected.get("visited_sum"), actual.getLongMetric("visited_sum"));
      Assert.assertEquals(
          (Double) expected.get("unique_hosts"),
          (Double) HyperUniquesAggregatorFactory.estimateCardinality(actual.getRaw("unique_hosts")),
          0.001
      );
    }
  }
}
