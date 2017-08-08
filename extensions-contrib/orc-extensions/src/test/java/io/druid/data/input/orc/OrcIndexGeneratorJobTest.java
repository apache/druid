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
package io.druid.data.input.orc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import io.druid.java.util.common.StringUtils;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.HadoopIOConfig;
import io.druid.indexer.HadoopIngestionSpec;
import io.druid.indexer.HadoopTuningConfig;
import io.druid.indexer.HadoopyShardSpec;
import io.druid.indexer.IndexGeneratorJob;
import io.druid.indexer.JobHelper;
import io.druid.indexer.Jobby;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexIndexableAdapter;
import io.druid.segment.Rowboat;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class OrcIndexGeneratorJobTest
{
  static private final AggregatorFactory[] aggs = {
      new LongSumAggregatorFactory("visited_num", "visited_num"),
      new HyperUniquesAggregatorFactory("unique_hosts", "host")
  };

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ObjectMapper mapper;
  private HadoopDruidIndexerConfig config;
  private final String dataSourceName = "website";
  private final List<String> data = ImmutableList.of(
          "2014102200,a.example.com,100",
          "2014102200,b.exmaple.com,50",
          "2014102200,c.example.com,200",
          "2014102200,d.example.com,250",
          "2014102200,e.example.com,123",
          "2014102200,f.example.com,567",
          "2014102200,g.example.com,11",
          "2014102200,h.example.com,251",
          "2014102200,i.example.com,963",
          "2014102200,j.example.com,333",
          "2014102212,a.example.com,100",
          "2014102212,b.exmaple.com,50",
          "2014102212,c.example.com,200",
          "2014102212,d.example.com,250",
          "2014102212,e.example.com,123",
          "2014102212,f.example.com,567",
          "2014102212,g.example.com,11",
          "2014102212,h.example.com,251",
          "2014102212,i.example.com,963",
          "2014102212,j.example.com,333"
      );
  private final Interval interval = new Interval("2014-10-22T00:00:00Z/P1D");
  private File dataRoot;
  private File outputRoot;
  private Integer[][][] shardInfoForEachSegment = new Integer[][][]{{
      {0, 4},
      {1, 4},
      {2, 4},
      {3, 4}
  }};
  private final InputRowParser inputRowParser = new OrcHadoopInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec("timestamp", "yyyyMMddHH", null),
          new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("host")), null, null)
      ),
      "struct<timestamp:string,host:string,visited_num:int>"
  );

  private File writeDataToLocalOrcFile(File outputDir, List<String> data) throws IOException
  {
    File outputFile = new File(outputDir, "test.orc");
    TypeDescription schema = TypeDescription.createStruct()
        .addField("timestamp", TypeDescription.createString())
        .addField("host", TypeDescription.createString())
        .addField("visited_num", TypeDescription.createInt());
    Configuration conf = new Configuration();
    Writer writer = OrcFile.createWriter(
        new Path(outputFile.getPath()),
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .bufferSize(10000)
            .compress(CompressionKind.ZLIB)
            .version(OrcFile.Version.CURRENT)
    );
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = data.size();
    for (int idx = 0; idx < data.size(); idx++) {
      String line = data.get(idx);
      String[] lineSplit = line.split(",");
      ((BytesColumnVector) batch.cols[0]).setRef(
          idx,
          StringUtils.toUtf8(lineSplit[0]),
          0,
          lineSplit[0].length()
      );
      ((BytesColumnVector) batch.cols[1]).setRef(
          idx,
          StringUtils.toUtf8(lineSplit[1]),
          0,
          lineSplit[1].length()
      );
      ((LongColumnVector) batch.cols[2]).vector[idx] = Long.parseLong(lineSplit[2]);
    }
    writer.addRowBatch(batch);
    writer.close();

    return outputFile;
  }

  @Before
  public void setUp() throws Exception
  {
    mapper = HadoopDruidIndexerConfig.JSON_MAPPER;
    mapper.registerSubtypes(new NamedType(HashBasedNumberedShardSpec.class, "hashed"));

    dataRoot = temporaryFolder.newFolder("data");
    outputRoot = temporaryFolder.newFolder("output");
    File dataFile = writeDataToLocalOrcFile(dataRoot, data);

    HashMap<String, Object> inputSpec = new HashMap<String, Object>();
    inputSpec.put("paths", dataFile.getCanonicalPath());
    inputSpec.put("type", "static");
    inputSpec.put("inputFormat", "org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat");

    config = new HadoopDruidIndexerConfig(
        new HadoopIngestionSpec(
            new DataSchema(
                dataSourceName,
                mapper.convertValue(
                    inputRowParser,
                    Map.class
                ),
                aggs,
                new UniformGranularitySpec(
                    Granularities.DAY, Granularities.NONE, ImmutableList.of(this.interval)
                ),
                mapper
            ),
            new HadoopIOConfig(
                ImmutableMap.copyOf(inputSpec),
                null,
                outputRoot.getCanonicalPath()
            ),
            new HadoopTuningConfig(
                outputRoot.getCanonicalPath(),
                null,
                null,
                null,
                null,
                null,
                false,
                false,
                false,
                false,
                ImmutableMap.of(JobContext.NUM_REDUCES, "0"), //verifies that set num reducers is ignored
                false,
                true,
                null,
                true,
                null,
                false,
                false,
                null
            )
        )
    );
    config.setShardSpecs(
        loadShardSpecs(shardInfoForEachSegment)
    );
    config = HadoopDruidIndexerConfig.fromSpec(config.getSchema());
  }

  @Test
  public void testIndexGeneratorJob() throws IOException
  {
    verifyJob(new IndexGeneratorJob(config));
  }

  private void verifyJob(IndexGeneratorJob job) throws IOException
  {
    JobHelper.runJobs(ImmutableList.<Jobby>of(job), config);

    int segmentNum = 0;
    for (DateTime currTime = interval.getStart(); currTime.isBefore(interval.getEnd()); currTime = currTime.plusDays(1)) {
      Integer[][] shardInfo = shardInfoForEachSegment[segmentNum++];
      File segmentOutputFolder = new File(
          StringUtils.format(
              "%s/%s/%s_%s/%s",
              config.getSchema().getIOConfig().getSegmentOutputPath(),
              config.getSchema().getDataSchema().getDataSource(),
              currTime.toString(),
              currTime.plusDays(1).toString(),
              config.getSchema().getTuningConfig().getVersion()
          )
      );
      Assert.assertTrue(segmentOutputFolder.exists());
      Assert.assertEquals(shardInfo.length, segmentOutputFolder.list().length);

      int rowCount = 0;
      for (int partitionNum = 0; partitionNum < shardInfo.length; ++partitionNum) {
        File individualSegmentFolder = new File(segmentOutputFolder, Integer.toString(partitionNum));
        Assert.assertTrue(individualSegmentFolder.exists());

        File descriptor = new File(individualSegmentFolder, "descriptor.json");
        File indexZip = new File(individualSegmentFolder, "index.zip");
        Assert.assertTrue(descriptor.exists());
        Assert.assertTrue(indexZip.exists());

        DataSegment dataSegment = mapper.readValue(descriptor, DataSegment.class);
        Assert.assertEquals(config.getSchema().getTuningConfig().getVersion(), dataSegment.getVersion());
        Assert.assertEquals(new Interval(currTime, currTime.plusDays(1)), dataSegment.getInterval());
        Assert.assertEquals("local", dataSegment.getLoadSpec().get("type"));
        Assert.assertEquals(indexZip.getCanonicalPath(), dataSegment.getLoadSpec().get("path"));
        Assert.assertEquals(Integer.valueOf(9), dataSegment.getBinaryVersion());

        Assert.assertEquals(dataSourceName, dataSegment.getDataSource());
        Assert.assertTrue(dataSegment.getDimensions().size() == 1);
        String[] dimensions = dataSegment.getDimensions().toArray(new String[dataSegment.getDimensions().size()]);
        Arrays.sort(dimensions);
        Assert.assertEquals("host", dimensions[0]);
        Assert.assertEquals("visited_num", dataSegment.getMetrics().get(0));
        Assert.assertEquals("unique_hosts", dataSegment.getMetrics().get(1));

        Integer[] hashShardInfo = shardInfo[partitionNum];
        HashBasedNumberedShardSpec spec = (HashBasedNumberedShardSpec) dataSegment.getShardSpec();
        Assert.assertEquals((int) hashShardInfo[0], spec.getPartitionNum());
        Assert.assertEquals((int) hashShardInfo[1], spec.getPartitions());

        File dir = Files.createTempDir();

        unzip(indexZip, dir);

        QueryableIndex index = HadoopDruidIndexerConfig.INDEX_IO.loadIndex(dir);
        QueryableIndexIndexableAdapter adapter = new QueryableIndexIndexableAdapter(index);

        for (Rowboat row: adapter.getRows()) {
          Object[] metrics = row.getMetrics();

          rowCount++;
          Assert.assertTrue(metrics.length == 2);
        }
      }
      Assert.assertEquals(rowCount, data.size());
    }
  }

  private Map<Long, List<HadoopyShardSpec>> loadShardSpecs(
      Integer[][][] shardInfoForEachShard
  )
  {
    Map<Long, List<HadoopyShardSpec>> shardSpecs = Maps.newTreeMap(DateTimeComparator.getInstance());
    int shardCount = 0;
    int segmentNum = 0;
    for (Interval segmentGranularity : config.getSegmentGranularIntervals().get()) {
      List<ShardSpec> specs = Lists.newArrayList();
      for (Integer[] shardInfo : shardInfoForEachShard[segmentNum++]) {
        specs.add(new HashBasedNumberedShardSpec(shardInfo[0], shardInfo[1], null, HadoopDruidIndexerConfig.JSON_MAPPER));
      }
      List<HadoopyShardSpec> actualSpecs = Lists.newArrayListWithExpectedSize(specs.size());
      for (ShardSpec spec : specs) {
        actualSpecs.add(new HadoopyShardSpec(spec, shardCount++));
      }

      shardSpecs.put(segmentGranularity.getStartMillis(), actualSpecs);
    }

    return shardSpecs;
  }

  private void unzip(File zip, File outDir)
  {
    try {
      long size = 0L;
      final byte[] buffer = new byte[1 << 13];
      try (ZipInputStream in = new ZipInputStream(new FileInputStream(zip))) {
        for (ZipEntry entry = in.getNextEntry(); entry != null; entry = in.getNextEntry()) {
          final String fileName = entry.getName();
          try (final OutputStream out = new BufferedOutputStream(
              new FileOutputStream(
                  outDir.getAbsolutePath()
                      + File.separator
                      + fileName
              ), 1 << 13
          )) {
            for (int len = in.read(buffer); len >= 0; len = in.read(buffer)) {
              if (len == 0) {
                continue;
              }
              size += len;
              out.write(buffer, 0, len);
            }
            out.flush();
          }
        }
      }
    }
    catch (IOException | RuntimeException exception) {
    }
  }
}
