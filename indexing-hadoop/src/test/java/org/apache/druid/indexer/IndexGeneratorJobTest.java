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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.HashPartitionFunction;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.SingleDimensionShardSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

@RunWith(Parameterized.class)
public class IndexGeneratorJobTest
{
  private static final AggregatorFactory[] AGGS1 = {
      new LongSumAggregatorFactory("visited_num", "visited_num"),
      new HyperUniquesAggregatorFactory("unique_hosts", "host")
  };

  private static final AggregatorFactory[] AGGS2 = {
      new CountAggregatorFactory("count")
  };

  @Parameterized.Parameters(name = "useCombiner={0}, partitionType={1}, interval={2}, shardInfoForEachSegment={3}, " +
                                   "data={4}, inputFormatName={5}, inputRowParser={6}, maxRowsInMemory={7}, " +
                                   "maxBytesInMemory={8}, aggs={9}, datasourceName={10}, forceExtendableShardSpecs={11}")
  public static Collection<Object[]> constructFeed()
  {
    final Object[][] baseConstructors = new Object[][]{
        {
            false,
            "single",
            "2014-10-22T00:00:00Z/P2D",
            new String[][][]{
                {
                    {null, "c.example.com"},
                    {"c.example.com", "e.example.com"},
                    {"e.example.com", "g.example.com"},
                    {"g.example.com", "i.example.com"},
                    {"i.example.com", null}
                },
                {
                    {null, "c.example.com"},
                    {"c.example.com", "e.example.com"},
                    {"e.example.com", "g.example.com"},
                    {"g.example.com", "i.example.com"},
                    {"i.example.com", null}
                }
            },
            ImmutableList.of(
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
                "2014102300,a.example.com,100",
                "2014102300,b.exmaple.com,50",
                "2014102300,c.example.com,200",
                "2014102300,d.example.com,250",
                "2014102300,e.example.com,123",
                "2014102300,f.example.com,567",
                "2014102300,g.example.com,11",
                "2014102300,h.example.com,251",
                "2014102300,i.example.com,963",
                "2014102300,j.example.com,333"
            ),
            null,
            new StringInputRowParser(
                new CSVParseSpec(
                    new TimestampSpec("timestamp", "yyyyMMddHH", null),
                    new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("host")), null, null),
                    null,
                    ImmutableList.of("timestamp", "host", "visited_num"),
                    false,
                    0
                ),
                null
            ),
            null,
            null,
            AGGS1,
            "website"
        },
        {
            false,
            "hashed",
            "2014-10-22T00:00:00Z/P1D",
            new Integer[][][]{
                {
                    {0, 4},
                    {1, 4},
                    {2, 4},
                    {3, 4}
                }
            },
            ImmutableList.of(
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
            null,
            new HadoopyStringInputRowParser(
                new CSVParseSpec(
                    new TimestampSpec("timestamp", "yyyyMMddHH", null),
                    new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("host")), null, null),
                    null,
                    ImmutableList.of("timestamp", "host", "visited_num"),
                    false,
                    0
                )
            ),
            null,
            null,
            AGGS1,
            "website"
        },
        {
            true,
            "hashed",
            "2014-10-22T00:00:00Z/P1D",
            new Integer[][][]{
                {
                    {0, 4},
                    {1, 4},
                    {2, 4},
                    {3, 4}
                }
            },
            ImmutableList.of(
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
            null,
            new StringInputRowParser(
                new CSVParseSpec(
                    new TimestampSpec("timestamp", "yyyyMMddHH", null),
                    new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("host")), null, null),
                    null,
                    ImmutableList.of("timestamp", "host", "visited_num"),
                    false,
                    0
                ),
                null
            ),
            null,
            null,
            AGGS1,
            "website"
        },
        {
            false,
            "single",
            "2014-10-22T00:00:00Z/P2D",
            new String[][][]{
                {
                    {null, "c.example.com"},
                    {"c.example.com", "e.example.com"},
                    {"e.example.com", "g.example.com"},
                    {"g.example.com", "i.example.com"},
                    {"i.example.com", null}
                },
                {
                    {null, "c.example.com"},
                    {"c.example.com", "e.example.com"},
                    {"e.example.com", "g.example.com"},
                    {"g.example.com", "i.example.com"},
                    {"i.example.com", null}
                }
            },
            ImmutableList.of(
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
                "2014102300,a.example.com,100",
                "2014102300,b.exmaple.com,50",
                "2014102300,c.example.com,200",
                "2014102300,d.example.com,250",
                "2014102300,e.example.com,123",
                "2014102300,f.example.com,567",
                "2014102300,g.example.com,11",
                "2014102300,h.example.com,251",
                "2014102300,i.example.com,963",
                "2014102300,j.example.com,333"
            ),
            SequenceFileInputFormat.class.getName(),
            new HadoopyStringInputRowParser(
                new CSVParseSpec(
                    new TimestampSpec("timestamp", "yyyyMMddHH", null),
                    new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("host")), null, null),
                    null,
                    ImmutableList.of("timestamp", "host", "visited_num"),
                    false,
                    0
                )
            ),
            null,
            null,
            AGGS1,
            "website"
        },
        {
            // Tests that new indexes inherit the dimension order from previous index
            false,
            "hashed",
            "2014-10-22T00:00:00Z/P1D",
            new Integer[][][]{
                {
                    {0, 1} // use a single partition, dimension order inheritance is not supported across partitions
                }
            },
            ImmutableList.of(
                "{\"ts\":\"2014102200\", \"X\":\"x.example.com\"}",
                "{\"ts\":\"2014102201\", \"Y\":\"y.example.com\"}",
                "{\"ts\":\"2014102202\", \"M\":\"m.example.com\"}",
                "{\"ts\":\"2014102203\", \"Q\":\"q.example.com\"}",
                "{\"ts\":\"2014102204\", \"B\":\"b.example.com\"}",
                "{\"ts\":\"2014102205\", \"F\":\"f.example.com\"}"
            ),
            null,
            new StringInputRowParser(
                new JSONParseSpec(
                    new TimestampSpec("ts", "yyyyMMddHH", null),
                    new DimensionsSpec(null, null, null),
                    null,
                    null,
                    null
                ),
                null
            ),
            1, // force 1 row max per index for easier testing
            null,
            AGGS2,
            "inherit_dims"
        },
        {
            // Tests that pre-specified dim order is maintained across indexes.
            false,
            "hashed",
            "2014-10-22T00:00:00Z/P1D",
            new Integer[][][]{
                {
                    {0, 1}
                }
            },
            ImmutableList.of(
                "{\"ts\":\"2014102200\", \"X\":\"x.example.com\"}",
                "{\"ts\":\"2014102201\", \"Y\":\"y.example.com\"}",
                "{\"ts\":\"2014102202\", \"M\":\"m.example.com\"}",
                "{\"ts\":\"2014102203\", \"Q\":\"q.example.com\"}",
                "{\"ts\":\"2014102204\", \"B\":\"b.example.com\"}",
                "{\"ts\":\"2014102205\", \"F\":\"f.example.com\"}"
            ),
            null,
            new StringInputRowParser(
                new JSONParseSpec(
                    new TimestampSpec("ts", "yyyyMMddHH", null),
                    new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of(
                        "B",
                        "F",
                        "M",
                        "Q",
                        "X",
                        "Y"
                    )), null, null),
                    null,
                    null,
                    null
                ),
                null
            ),
            1, // force 1 row max per index for easier testing
            null,
            AGGS2,
            "inherit_dims2"
        }
    };

    // Run each baseConstructor with/without forceExtendableShardSpecs.
    final List<Object[]> constructors = new ArrayList<>();
    for (Object[] baseConstructor : baseConstructors) {
      for (int forceExtendableShardSpecs = 0; forceExtendableShardSpecs < 2; forceExtendableShardSpecs++) {
        final Object[] fullConstructor = new Object[baseConstructor.length + 1];
        System.arraycopy(baseConstructor, 0, fullConstructor, 0, baseConstructor.length);
        fullConstructor[baseConstructor.length] = forceExtendableShardSpecs == 0;
        constructors.add(fullConstructor);
      }
    }

    return constructors;
  }

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final boolean useCombiner;
  private final String partitionType;
  private final Interval interval;
  private final Object[][][] shardInfoForEachSegment;
  private final List<String> data;
  private final String inputFormatName;
  private final InputRowParser inputRowParser;
  private final Integer maxRowsInMemory;
  private final Long maxBytesInMemory;
  private final AggregatorFactory[] aggs;
  private final String datasourceName;
  private final boolean forceExtendableShardSpecs;

  private ObjectMapper mapper;
  private HadoopDruidIndexerConfig config;
  private File dataFile;
  private File tmpDir;

  public IndexGeneratorJobTest(
      boolean useCombiner,
      String partitionType,
      String interval,
      Object[][][] shardInfoForEachSegment,
      List<String> data,
      String inputFormatName,
      InputRowParser inputRowParser,
      Integer maxRowsInMemory,
      Long maxBytesInMemory,
      AggregatorFactory[] aggs,
      String datasourceName,
      boolean forceExtendableShardSpecs
  )
  {
    this.useCombiner = useCombiner;
    this.partitionType = partitionType;
    this.shardInfoForEachSegment = shardInfoForEachSegment;
    this.interval = Intervals.of(interval);
    this.data = data;
    this.inputFormatName = inputFormatName;
    this.inputRowParser = inputRowParser;
    this.maxRowsInMemory = maxRowsInMemory;
    this.maxBytesInMemory = maxBytesInMemory;
    this.aggs = aggs;
    this.datasourceName = datasourceName;
    this.forceExtendableShardSpecs = forceExtendableShardSpecs;
  }

  private void writeDataToLocalSequenceFile(File outputFile, List<String> data) throws IOException
  {
    Configuration conf = new Configuration();
    LocalFileSystem fs = FileSystem.getLocal(conf);
    Writer fileWriter = SequenceFile.createWriter(
        fs,
        conf,
        new Path(outputFile.getAbsolutePath()),
        BytesWritable.class,
        BytesWritable.class,
        SequenceFile.CompressionType.NONE,
        (CompressionCodec) null
    );

    int keyCount = 10;
    for (String line : data) {
      ByteBuffer buf = ByteBuffer.allocate(4);
      buf.putInt(keyCount);
      BytesWritable key = new BytesWritable(buf.array());
      BytesWritable value = new BytesWritable(StringUtils.toUtf8(line));
      fileWriter.append(key, value);
      keyCount += 1;
    }

    fileWriter.close();
  }

  @Before
  public void setUp() throws Exception
  {
    mapper = HadoopDruidIndexerConfig.JSON_MAPPER;
    mapper.registerSubtypes(new NamedType(HashBasedNumberedShardSpec.class, "hashed"));
    mapper.registerSubtypes(new NamedType(SingleDimensionShardSpec.class, "single"));

    dataFile = temporaryFolder.newFile();
    tmpDir = temporaryFolder.newFolder();

    HashMap<String, Object> inputSpec = new HashMap<String, Object>();
    inputSpec.put("paths", dataFile.getCanonicalPath());
    inputSpec.put("type", "static");
    if (inputFormatName != null) {
      inputSpec.put("inputFormat", inputFormatName);
    }

    if (SequenceFileInputFormat.class.getName().equals(inputFormatName)) {
      writeDataToLocalSequenceFile(dataFile, data);
    } else {
      FileUtils.writeLines(dataFile, data);
    }

    config = new HadoopDruidIndexerConfig(
        new HadoopIngestionSpec(
            new DataSchema(
                datasourceName,
                mapper.convertValue(
                    inputRowParser,
                    Map.class
                ),
                aggs,
                new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, ImmutableList.of(this.interval)),
                null,
                mapper
            ),
            new HadoopIOConfig(
                ImmutableMap.copyOf(inputSpec),
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
                maxRowsInMemory,
                maxBytesInMemory,
                true,
                false,
                false,
                false,
                ImmutableMap.of(MRJobConfig.NUM_REDUCES, "0"), //verifies that set num reducers is ignored
                false,
                useCombiner,
                null,
                true,
                null,
                forceExtendableShardSpecs,
                false,
                null,
                null,
                null,
                null
            )
        )
    );

    config.setShardSpecs(loadShardSpecs(partitionType, shardInfoForEachSegment));
    config = HadoopDruidIndexerConfig.fromSpec(config.getSchema());
  }

  private List<ShardSpec> constructShardSpecFromShardInfo(String partitionType, Object[][] shardInfoForEachShard)
  {
    List<ShardSpec> specs = new ArrayList<>();
    if ("hashed".equals(partitionType)) {
      for (Integer[] shardInfo : (Integer[][]) shardInfoForEachShard) {
        specs.add(
            new HashBasedNumberedShardSpec(
                shardInfo[0],
                shardInfo[1],
                shardInfo[0],
                shardInfo[1],
                null,
                HashPartitionFunction.MURMUR3_32_ABS,
                HadoopDruidIndexerConfig.JSON_MAPPER
            )
        );
      }
    } else if ("single".equals(partitionType)) {
      int partitionNum = 0;
      for (String[] shardInfo : (String[][]) shardInfoForEachShard) {
        specs.add(new SingleDimensionShardSpec(
            "host",
            shardInfo[0],
            shardInfo[1],
            partitionNum++,
            shardInfoForEachShard.length
        ));
      }
    } else {
      throw new RE("Invalid partition type:[%s]", partitionType);
    }

    return specs;
  }

  private Map<Long, List<HadoopyShardSpec>> loadShardSpecs(
      String partitionType,
      Object[][][] shardInfoForEachShard
  )
  {
    Map<Long, List<HadoopyShardSpec>> shardSpecs = new TreeMap<>(DateTimeComparator.getInstance());
    int shardCount = 0;
    int segmentNum = 0;
    for (Interval segmentGranularity : config.getSegmentGranularIntervals().get()) {
      List<ShardSpec> specs = constructShardSpecFromShardInfo(partitionType, shardInfoForEachShard[segmentNum++]);
      List<HadoopyShardSpec> actualSpecs = Lists.newArrayListWithExpectedSize(specs.size());
      for (ShardSpec spec : specs) {
        actualSpecs.add(new HadoopyShardSpec(spec, shardCount++));
      }

      shardSpecs.put(segmentGranularity.getStartMillis(), actualSpecs);
    }

    return shardSpecs;
  }

  @Test
  public void testIndexGeneratorJob() throws IOException
  {
    verifyJob(new IndexGeneratorJob(config));
  }

  private void verifyJob(IndexGeneratorJob job) throws IOException
  {
    Assert.assertTrue(JobHelper.runJobs(ImmutableList.of(job), config));

    final Map<Interval, List<DataSegment>> intervalToSegments = new HashMap<>();
    IndexGeneratorJob
        .getPublishedSegments(config)
        .forEach(segment -> intervalToSegments.computeIfAbsent(segment.getInterval(), k -> new ArrayList<>())
                                              .add(segment));

    final Map<Interval, List<File>> intervalToIndexFiles = new HashMap<>();
    int segmentNum = 0;
    for (DateTime currTime = interval.getStart(); currTime.isBefore(interval.getEnd()); currTime = currTime.plusDays(1)) {
      Object[][] shardInfo = shardInfoForEachSegment[segmentNum++];
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

      for (int partitionNum = 0; partitionNum < shardInfo.length; ++partitionNum) {
        File individualSegmentFolder = new File(segmentOutputFolder, Integer.toString(partitionNum));
        Assert.assertTrue(individualSegmentFolder.exists());

        File indexZip = new File(individualSegmentFolder, "index.zip");
        Assert.assertTrue(indexZip.exists());

        intervalToIndexFiles.computeIfAbsent(new Interval(currTime, currTime.plusDays(1)), k -> new ArrayList<>())
                            .add(indexZip);
      }
    }

    Assert.assertEquals(intervalToSegments.size(), intervalToIndexFiles.size());

    segmentNum = 0;
    for (Entry<Interval, List<DataSegment>> entry : intervalToSegments.entrySet()) {
      final Interval interval = entry.getKey();
      final List<DataSegment> segments = entry.getValue();
      final List<File> indexFiles = intervalToIndexFiles.get(interval);
      Collections.sort(segments);
      indexFiles.sort(Comparator.comparing(File::getAbsolutePath));

      Assert.assertNotNull(indexFiles);
      Assert.assertEquals(segments.size(), indexFiles.size());

      Object[][] shardInfo = shardInfoForEachSegment[segmentNum++];

      for (int i = 0; i < segments.size(); i++) {
        final DataSegment dataSegment = segments.get(i);
        final File indexZip = indexFiles.get(i);

        Assert.assertEquals(config.getSchema().getTuningConfig().getVersion(), dataSegment.getVersion());
        Assert.assertEquals("local", dataSegment.getLoadSpec().get("type"));
        Assert.assertEquals(indexZip.getCanonicalPath(), dataSegment.getLoadSpec().get("path"));
        Assert.assertEquals(Integer.valueOf(9), dataSegment.getBinaryVersion());

        if ("website".equals(datasourceName)) {
          Assert.assertEquals("website", dataSegment.getDataSource());
          Assert.assertEquals("host", dataSegment.getDimensions().get(0));
          Assert.assertEquals("visited_num", dataSegment.getMetrics().get(0));
          Assert.assertEquals("unique_hosts", dataSegment.getMetrics().get(1));
        } else if ("inherit_dims".equals(datasourceName)) {
          Assert.assertEquals("inherit_dims", dataSegment.getDataSource());
          Assert.assertEquals(ImmutableList.of("X", "Y", "M", "Q", "B", "F"), dataSegment.getDimensions());
          Assert.assertEquals("count", dataSegment.getMetrics().get(0));
        } else if ("inherit_dims2".equals(datasourceName)) {
          Assert.assertEquals("inherit_dims2", dataSegment.getDataSource());
          Assert.assertEquals(ImmutableList.of("B", "F", "M", "Q", "X", "Y"), dataSegment.getDimensions());
          Assert.assertEquals("count", dataSegment.getMetrics().get(0));
        } else {
          Assert.fail("Test did not specify supported datasource name");
        }

        if (forceExtendableShardSpecs) {
          NumberedShardSpec spec = (NumberedShardSpec) dataSegment.getShardSpec();
          Assert.assertEquals(i, spec.getPartitionNum());
          Assert.assertEquals(shardInfo.length, spec.getNumCorePartitions());
        } else if ("hashed".equals(partitionType)) {
          Integer[] hashShardInfo = (Integer[]) shardInfo[i];
          HashBasedNumberedShardSpec spec = (HashBasedNumberedShardSpec) dataSegment.getShardSpec();
          Assert.assertEquals((int) hashShardInfo[0], spec.getPartitionNum());
          Assert.assertEquals((int) hashShardInfo[1], spec.getNumCorePartitions());
        } else if ("single".equals(partitionType)) {
          String[] singleDimensionShardInfo = (String[]) shardInfo[i];
          SingleDimensionShardSpec spec = (SingleDimensionShardSpec) dataSegment.getShardSpec();
          Assert.assertEquals(singleDimensionShardInfo[0], spec.getStart());
          Assert.assertEquals(singleDimensionShardInfo[1], spec.getEnd());
        } else {
          throw new RE("Invalid partition type:[%s]", partitionType);
        }
      }
    }
  }
}
