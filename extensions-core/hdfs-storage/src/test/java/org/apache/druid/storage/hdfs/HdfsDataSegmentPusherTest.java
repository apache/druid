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

package org.apache.druid.storage.hdfs;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.apache.druid.indexer.Bucket;
import org.apache.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.druid.indexer.HadoopIngestionSpec;
import org.apache.druid.indexer.JobHelper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.jackson.GranularityModule;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.loading.LocalDataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.DataSegment.PruneSpecsHolder;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.regex.Pattern;

/**
 */
public class HdfsDataSegmentPusherTest
{
  static TestObjectMapper objectMapper;

  static {
    objectMapper = new TestObjectMapper();
    InjectableValues.Std injectableValues = new InjectableValues.Std();
    injectableValues.addValue(ObjectMapper.class, objectMapper);
    injectableValues.addValue(PruneSpecsHolder.class, PruneSpecsHolder.DEFAULT);
    objectMapper.setInjectableValues(injectableValues);
  }

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private HdfsDataSegmentPusher hdfsDataSegmentPusher;

  @Before
  public void setUp()
  {
    HdfsDataSegmentPusherConfig hdfsDataSegmentPusherConf = new HdfsDataSegmentPusherConfig();
    hdfsDataSegmentPusherConf.setStorageDirectory("path/to/");
    hdfsDataSegmentPusher = new HdfsDataSegmentPusher(hdfsDataSegmentPusherConf, new Configuration(true), objectMapper);
  }

  @Test
  public void testPushWithScheme() throws Exception
  {
    testUsingScheme("file");
  }

  @Test
  public void testPushWithBadScheme() throws Exception
  {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("No FileSystem for scheme");
    testUsingScheme("xyzzy");

    // Not reached
    Assert.assertTrue(false);
  }

  @Test
  public void testPushWithoutScheme() throws Exception
  {
    testUsingScheme(null);
  }

  @Test
  public void testPushWithMultipleSegments() throws Exception
  {
    testUsingSchemeForMultipleSegments("file", 3);
  }

  @Test
  public void testUsingUniqueFilePath() throws Exception
  {
    Configuration conf = new Configuration(true);

    // Create a mock segment on disk
    File segmentDir = tempFolder.newFolder();
    File tmp = new File(segmentDir, "version.bin");

    final byte[] data = new byte[]{0x0, 0x0, 0x0, 0x1};
    Files.write(data, tmp);
    final long size = data.length;

    HdfsDataSegmentPusherConfig config = new HdfsDataSegmentPusherConfig();
    final File storageDirectory = tempFolder.newFolder();

    config.setStorageDirectory(StringUtils.format("file://%s", storageDirectory.getAbsolutePath()));
    HdfsDataSegmentPusher pusher = new HdfsDataSegmentPusher(config, conf, new DefaultObjectMapper());

    DataSegment segmentToPush = new DataSegment(
        "foo",
        Intervals.of("2015/2016"),
        "0",
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        0,
        size
    );

    DataSegment segment = pusher.push(segmentDir, segmentToPush, true);

    Pattern pattern =
        Pattern.compile(".*/foo/20150101T000000\\.000Z_20160101T000000\\.000Z/0/0_[A-Za-z0-9-]{36}_index\\.zip");
    Assert.assertTrue(
        segment.getLoadSpec().get("path").toString(),
        pattern.matcher(segment.getLoadSpec().get("path").toString()).matches()
    );
  }

  private void testUsingSchemeForMultipleSegments(final String scheme, final int numberOfSegments) throws Exception
  {
    Configuration conf = new Configuration(true);
    DataSegment[] segments = new DataSegment[numberOfSegments];

    // Create a mock segment on disk
    File segmentDir = tempFolder.newFolder();
    File tmp = new File(segmentDir, "version.bin");

    final byte[] data = new byte[]{0x0, 0x0, 0x0, 0x1};
    Files.write(data, tmp);
    final long size = data.length;

    HdfsDataSegmentPusherConfig config = new HdfsDataSegmentPusherConfig();
    final File storageDirectory = tempFolder.newFolder();

    config.setStorageDirectory(
        scheme != null
        ? StringUtils.format("%s://%s", scheme, storageDirectory.getAbsolutePath())
        : storageDirectory.getAbsolutePath()
    );
    HdfsDataSegmentPusher pusher = new HdfsDataSegmentPusher(config, conf, new DefaultObjectMapper());

    for (int i = 0; i < numberOfSegments; i++) {
      segments[i] = new DataSegment(
          "foo",
          Intervals.of("2015/2016"),
          "0",
          new HashMap<>(),
          new ArrayList<>(),
          new ArrayList<>(),
          new NumberedShardSpec(i, i),
          0,
          size
      );
    }

    for (int i = 0; i < numberOfSegments; i++) {
      final DataSegment pushedSegment = pusher.push(segmentDir, segments[i], false);

      String indexUri = StringUtils.format(
          "%s/%s/%d_index.zip",
          FileSystem.newInstance(conf).makeQualified(new Path(config.getStorageDirectory())).toUri().toString(),
          pusher.getStorageDir(segments[i], false),
          segments[i].getShardSpec().getPartitionNum()
      );

      Assert.assertEquals(segments[i].getSize(), pushedSegment.getSize());
      Assert.assertEquals(segments[i], pushedSegment);
      Assert.assertEquals(ImmutableMap.of(
          "type",
          "hdfs",
          "path",
          indexUri
      ), pushedSegment.getLoadSpec());
      // rename directory after push
      String segmentPath = pusher.getStorageDir(pushedSegment, false);

      File indexFile = new File(StringUtils.format(
          "%s/%s/%d_index.zip",
          storageDirectory,
          segmentPath,
          pushedSegment.getShardSpec().getPartitionNum()
      ));
      Assert.assertTrue(indexFile.exists());

      Assert.assertEquals(segments[i].getSize(), pushedSegment.getSize());
      Assert.assertEquals(segments[i], pushedSegment);

      indexFile = new File(StringUtils.format(
          "%s/%s/%d_index.zip",
          storageDirectory,
          segmentPath,
          pushedSegment.getShardSpec().getPartitionNum()
      ));
      Assert.assertTrue(indexFile.exists());


      // push twice will fail and temp dir cleaned
      File outDir = new File(StringUtils.format("%s/%s", config.getStorageDirectory(), segmentPath));
      outDir.setReadOnly();
      try {
        pusher.push(segmentDir, segments[i], false);
      }
      catch (IOException e) {
        Assert.fail("should not throw exception");
      }
    }
  }

  private void testUsingScheme(final String scheme) throws Exception
  {
    Configuration conf = new Configuration(true);

    // Create a mock segment on disk
    File segmentDir = tempFolder.newFolder();
    File tmp = new File(segmentDir, "version.bin");

    final byte[] data = new byte[]{0x0, 0x0, 0x0, 0x1};
    Files.write(data, tmp);
    final long size = data.length;

    HdfsDataSegmentPusherConfig config = new HdfsDataSegmentPusherConfig();
    final File storageDirectory = tempFolder.newFolder();

    config.setStorageDirectory(
        scheme != null
        ? StringUtils.format("%s://%s", scheme, storageDirectory.getAbsolutePath())
        : storageDirectory.getAbsolutePath()
    );
    HdfsDataSegmentPusher pusher = new HdfsDataSegmentPusher(config, conf, new DefaultObjectMapper());

    DataSegment segmentToPush = new DataSegment(
        "foo",
        Intervals.of("2015/2016"),
        "0",
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        0,
        size
    );

    DataSegment segment = pusher.push(segmentDir, segmentToPush, false);


    String indexUri = StringUtils.format(
        "%s/%s/%d_index.zip",
        FileSystem.newInstance(conf).makeQualified(new Path(config.getStorageDirectory())).toUri().toString(),
        pusher.getStorageDir(segmentToPush, false),
        segmentToPush.getShardSpec().getPartitionNum()
    );

    Assert.assertEquals(segmentToPush.getSize(), segment.getSize());
    Assert.assertEquals(segmentToPush, segment);
    Assert.assertEquals(ImmutableMap.of(
        "type",
        "hdfs",
        "path",
        indexUri
    ), segment.getLoadSpec());
    // rename directory after push
    final String segmentPath = pusher.getStorageDir(segment, false);

    File indexFile = new File(StringUtils.format(
        "%s/%s/%d_index.zip",
        storageDirectory,
        segmentPath,
        segment.getShardSpec().getPartitionNum()
    ));
    Assert.assertTrue(indexFile.exists());

    // push twice will fail and temp dir cleaned
    File outDir = new File(StringUtils.format("%s/%s", config.getStorageDirectory(), segmentPath));
    outDir.setReadOnly();
    try {
      pusher.push(segmentDir, segmentToPush, false);
    }
    catch (IOException e) {
      Assert.fail("should not throw exception");
    }
  }

  public static class TestObjectMapper extends ObjectMapper
  {
    public TestObjectMapper()
    {
      configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      configure(MapperFeature.AUTO_DETECT_GETTERS, false);
      configure(MapperFeature.AUTO_DETECT_FIELDS, false);
      configure(MapperFeature.AUTO_DETECT_IS_GETTERS, false);
      configure(MapperFeature.AUTO_DETECT_SETTERS, false);
      configure(SerializationFeature.INDENT_OUTPUT, false);
      configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
      registerModule(new TestModule().registerSubtypes(new NamedType(NumberedShardSpec.class, "NumberedShardSpec")));
      registerModule(new GranularityModule());
    }

    public static class TestModule extends SimpleModule
    {
      TestModule()
      {
        addSerializer(Interval.class, ToStringSerializer.instance);
        addSerializer(NumberedShardSpec.class, ToStringSerializer.instance);
        addDeserializer(
            Interval.class,
            new StdDeserializer<Interval>(Interval.class)
            {
              @Override
              public Interval deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                  throws IOException
              {
                return Intervals.of(jsonParser.getText());
              }
            }
        );
      }
    }
  }

  @Test
  public void shouldNotHaveColonsInHdfsStorageDir()
  {

    Interval interval = Intervals.of("2011-10-01/2011-10-02");
    ImmutableMap<String, Object> loadSpec = ImmutableMap.of("something", "or_other");

    DataSegment segment = new DataSegment(
        "something",
        interval,
        "brand:new:version",
        loadSpec,
        Arrays.asList("dim1", "dim2"),
        Arrays.asList("met1", "met2"),
        NoneShardSpec.instance(),
        null,
        1
    );

    String storageDir = hdfsDataSegmentPusher.getStorageDir(segment, false);
    Assert.assertEquals("something/20111001T000000.000Z_20111002T000000.000Z/brand_new_version", storageDir);

  }


  @Test
  public void shouldMakeHDFSCompliantSegmentOutputPath()
  {
    HadoopIngestionSpec schema;

    try {
      schema = objectMapper.readValue(
      "{\n"
            + "    \"dataSchema\": {\n"
            + "        \"dataSource\": \"source\",\n"
            + "        \"metricsSpec\": [],\n"
            + "        \"granularitySpec\": {\n"
            + "            \"type\": \"uniform\",\n"
            + "            \"segmentGranularity\": \"hour\",\n"
            + "            \"intervals\": [\"2012-07-10/P1D\"]\n"
            + "        }\n"
            + "    },\n"
            + "    \"ioConfig\": {\n"
            + "        \"type\": \"hadoop\",\n"
            + "        \"segmentOutputPath\": \"hdfs://server:9100/tmp/druid/datatest\"\n"
            + "    }\n"
            + "}",
          HadoopIngestionSpec.class
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

    //DataSchema dataSchema = new DataSchema("dataSource", null, null, Gra)
    //schema = new HadoopIngestionSpec(dataSchema, ioConfig, HadoopTuningConfig.makeDefaultTuningConfig());
    HadoopDruidIndexerConfig cfg = new HadoopDruidIndexerConfig(
        schema.withTuningConfig(
            schema.getTuningConfig()
                  .withVersion(
                      "some:brand:new:version"
                  )
        )
    );

    Bucket bucket = new Bucket(4711, new DateTime(2012, 07, 10, 5, 30, ISOChronology.getInstanceUTC()), 4712);
    Path path = JobHelper.makeFileNamePath(
        new Path(cfg.getSchema().getIOConfig().getSegmentOutputPath()),
        new DistributedFileSystem(),
        new DataSegment(
            cfg.getSchema().getDataSchema().getDataSource(),
            cfg.getSchema().getDataSchema().getGranularitySpec().bucketInterval(bucket.time).get(),
            cfg.getSchema().getTuningConfig().getVersion(),
            null,
            null,
            null,
            new NumberedShardSpec(bucket.partitionNum, 5000),
            -1,
            -1
        ),
        JobHelper.INDEX_ZIP,
        hdfsDataSegmentPusher
    );
    Assert.assertEquals(
        "hdfs://server:9100/tmp/druid/datatest/source/20120710T050000.000Z_20120710T060000.000Z/some_brand_new_version"
        + "/4712_index.zip",
        path.toString()
    );

    path = JobHelper.makeTmpPath(
        new Path(cfg.getSchema().getIOConfig().getSegmentOutputPath()),
        new DistributedFileSystem(),
        new DataSegment(
            cfg.getSchema().getDataSchema().getDataSource(),
            cfg.getSchema().getDataSchema().getGranularitySpec().bucketInterval(bucket.time).get(),
            cfg.getSchema().getTuningConfig().getVersion(),
            null,
            null,
            null,
            new NumberedShardSpec(bucket.partitionNum, 5000),
            -1,
            -1
        ),
        new TaskAttemptID("abc", 123, TaskType.REDUCE, 1, 0),
        hdfsDataSegmentPusher
    );
    Assert.assertEquals(
        "hdfs://server:9100/tmp/druid/datatest/source/20120710T050000.000Z_20120710T060000.000Z/some_brand_new_version"
        + "/4712_index.zip.0",
        path.toString()
    );

  }

  @Test
  public void shouldMakeDefaultSegmentOutputPathIfNotHDFS()
  {
    final HadoopIngestionSpec schema;

    try {
      schema = objectMapper.readValue(
          "{\n"
          + "    \"dataSchema\": {\n"
          + "        \"dataSource\": \"the:data:source\",\n"
          + "        \"metricsSpec\": [],\n"
          + "        \"granularitySpec\": {\n"
          + "            \"type\": \"uniform\",\n"
          + "            \"segmentGranularity\": \"hour\",\n"
          + "            \"intervals\": [\"2012-07-10/P1D\"]\n"
          + "        }\n"
          + "    },\n"
          + "    \"ioConfig\": {\n"
          + "        \"type\": \"hadoop\",\n"
          + "        \"segmentOutputPath\": \"/tmp/dru:id/data:test\"\n"
          + "    }\n"
          + "}",
          HadoopIngestionSpec.class
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

    HadoopDruidIndexerConfig cfg = new HadoopDruidIndexerConfig(
        schema.withTuningConfig(
            schema.getTuningConfig()
                  .withVersion(
                      "some:brand:new:version"
                  )
        )
    );

    Bucket bucket = new Bucket(4711, new DateTime(2012, 07, 10, 5, 30, ISOChronology.getInstanceUTC()), 4712);
    Path path = JobHelper.makeFileNamePath(
        new Path(cfg.getSchema().getIOConfig().getSegmentOutputPath()),
        new LocalFileSystem(),
        new DataSegment(
            cfg.getSchema().getDataSchema().getDataSource(),
            cfg.getSchema().getDataSchema().getGranularitySpec().bucketInterval(bucket.time).get(),
            cfg.getSchema().getTuningConfig().getVersion(),
            null,
            null,
            null,
            new NumberedShardSpec(bucket.partitionNum, 5000),
            -1,
            -1
        ),
        JobHelper.INDEX_ZIP,
        new LocalDataSegmentPusher(new LocalDataSegmentPusherConfig())
    );
    Assert.assertEquals(
        "file:/tmp/dru:id/data:test/the:data:source/2012-07-10T05:00:00.000Z_2012-07-10T06:00:00.000Z/some:brand:new:"
        + "version/4712/index.zip",
        path.toString()
    );

    path = JobHelper.makeTmpPath(
        new Path(cfg.getSchema().getIOConfig().getSegmentOutputPath()),
        new LocalFileSystem(),
        new DataSegment(
            cfg.getSchema().getDataSchema().getDataSource(),
            cfg.getSchema().getDataSchema().getGranularitySpec().bucketInterval(bucket.time).get(),
            cfg.getSchema().getTuningConfig().getVersion(),
            null,
            null,
            null,
            new NumberedShardSpec(bucket.partitionNum, 5000),
            -1,
            -1
        ),
        new TaskAttemptID("abc", 123, TaskType.REDUCE, 1, 0),
        new LocalDataSegmentPusher(new LocalDataSegmentPusherConfig())
    );
    Assert.assertEquals(
        "file:/tmp/dru:id/data:test/the:data:source/2012-07-10T05:00:00.000Z_2012-07-10T06:00:00.000Z/some:brand:new:"
        + "version/4712/index.zip.0",
        path.toString()
    );

  }
}
