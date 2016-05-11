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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.metamx.common.Granularity;
import io.druid.data.input.impl.CSVParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexIndexableAdapter;
import io.druid.segment.Rowboat;
import io.druid.segment.data.Indexed;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapreduce.JobContext;
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

public class IndexGeneratorJobPartitionDirTest
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
  private final Map<String, List<String>> data = ImmutableMap.<String, List<String>>of(
      "test1=a/test2=1", ImmutableList.of(
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
      ),
      "test1=a/test2=2", ImmutableList.of(
          "2014102200,a.example.com,100",
          "2014102201,b.exmaple.com,50",
          "2014102202,c.example.com,200",
          "2014102203,d.example.com,250",
          "2014102204,e.example.com,123",
          "2014102205,f.example.com,567",
          "2014102206,g.example.com,11",
          "2014102207,h.example.com,251",
          "2014102208,i.example.com,963",
          "2014102209,j.example.com,333",
          "2014102210,k.example.com,253",
          "2014102211,l.example.com,321",
          "2014102212,m.example.com,3125",
          "2014102213,n.example.com,234",
          "2014102214,o.example.com,325",
          "2014102215,p.example.com,3533",
          "2014102216,q.example.com,500",
          "2014102216,q.example.com,87"
      ),
      "test1=b/test2=1", ImmutableList.of(
          "2014102200,a.example.com,100",
          "2014102201,b.exmaple.com,50",
          "2014102202,c.example.com,200",
          "2014102203,d.example.com,250",
          "2014102204,e.example.com,123",
          "2014102205,f.example.com,567",
          "2014102206,g.example.com,11",
          "2014102207,h.example.com,251",
          "2014102208,i.example.com,963",
          "2014102209,j.example.com,333",
          "2014102210,k.example.com,253",
          "2014102211,l.example.com,321",
          "2014102212,m.example.com,3125",
          "2014102213,n.example.com,234",
          "2014102214,o.example.com,325",
          "2014102215,p.example.com,3533",
          "2014102216,q.example.com,500",
          "2014102216,q.example.com,87"
      ),
      "test1=b/testx=3", ImmutableList.of(
          "2014102200,a.example.com,100",
          "2014102201,b.exmaple.com,50",
          "2014102202,c.example.com,200",
          "2014102203,d.example.com,250",
          "2014102204,e.example.com,123",
          "2014102205,f.example.com,567",
          "2014102206,g.example.com,11",
          "2014102207,h.example.com,251",
          "2014102208,i.example.com,963",
          "2014102209,j.example.com,333",
          "2014102210,k.example.com,253",
          "2014102211,l.example.com,321",
          "2014102212,m.example.com,3125",
          "2014102213,n.example.com,234",
          "2014102214,o.example.com,325",
          "2014102215,p.example.com,3533",
          "2014102216,q.example.com,500",
          "2014102216,q.example.com,87"
      )
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
  private final InputRowParser inputRowParser = new HadoopyStringInputRowParser(
      new CSVParseSpec(
          new TimestampSpec("timestamp", "yyyyMMddHH", null),
          new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("host")), null, null),
          null,
          ImmutableList.of("timestamp", "host", "visited_num")
      )
  );

  @Before
  public void setUp() throws Exception
  {
    mapper = HadoopDruidIndexerConfig.JSON_MAPPER;
    mapper.registerSubtypes(new NamedType(HashBasedNumberedShardSpec.class, "hashed"));

    dataRoot = temporaryFolder.newFolder("data=hear");
    outputRoot = temporaryFolder.newFolder("output");

    for (Map.Entry<String, List<String>> entry: data.entrySet()) {
      temporaryFolder.newFolder(("data=hear/" + entry.getKey()).split("/"));
      File dataFile = temporaryFolder.newFile("data=hear/" + entry.getKey() + "/data");
      FileUtils.writeLines(dataFile, entry.getValue());
    }

    HashMap<String, Object> inputSpec = new HashMap<>();
    inputSpec.put("type", "partition");
    inputSpec.put("basePath", dataRoot.getCanonicalPath());
    inputSpec.put("partitionColumns", ImmutableList.of("test1", "test2"));

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
                    Granularity.DAY, QueryGranularities.NONE, ImmutableList.of(this.interval)
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
          String.format(
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
        Assert.assertTrue(dataSegment.getDimensions().size() == 3);
        String[] dimensions = dataSegment.getDimensions().toArray(new String[dataSegment.getDimensions().size()]);
        Arrays.sort(dimensions);
        Assert.assertEquals("host", dimensions[0]);
        Assert.assertEquals("test1", dimensions[1]);
        Assert.assertEquals("test2", dimensions[2]);
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

        Indexed<String> hostString = adapter.getDimValueLookup("host");
        int hostIndex = adapter.getDimensionNames().indexOf("host");
        Indexed<String> test1String = adapter.getDimValueLookup("test1");
        int test1Index = adapter.getDimensionNames().indexOf("test1");
        Indexed<String> test2String = adapter.getDimValueLookup("test2");
        int test2Index = adapter.getDimensionNames().indexOf("test2");
        List<String> test1Expected = ImmutableList.of("a", "b");
        List<String> test2Expected = ImmutableList.of("1", "2");

        for(Rowboat row: adapter.getRows())
        {
          Object[] metrics = row.getMetrics();
          int[][] dimInts = row.getDims();
          String host = hostString.get(dimInts[hostIndex][0]);
          String test1 = test1String.get(dimInts[test1Index][0]);
          String test2 = test2String.get(dimInts[test2Index][0]);

          Assert.assertTrue(metrics.length == 2);
          Assert.assertTrue(test1Expected.contains(test1));
          Assert.assertTrue(test2Expected.contains(test2));
        }
      }
    }
  }

  private Map<DateTime, List<HadoopyShardSpec>> loadShardSpecs(
      Integer[][][] shardInfoForEachShard
  )
  {
    Map<DateTime, List<HadoopyShardSpec>> shardSpecs = Maps.newTreeMap(DateTimeComparator.getInstance());
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

      shardSpecs.put(segmentGranularity.getStart(), actualSpecs);
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
