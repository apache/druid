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

package io.druid.storage.hdfs;

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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import io.druid.indexer.Bucket;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.HadoopIngestionSpec;
import io.druid.indexer.JobHelper;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.jackson.GranularityModule;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.StringUtils;
import io.druid.segment.loading.LocalDataSegmentPusher;
import io.druid.segment.loading.LocalDataSegmentPusherConfig;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.NumberedShardSpec;
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
import java.util.Arrays;

/**
 */
public class HdfsDataSegmentPusherTest
{

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  static TestObjectMapper objectMapper = new TestObjectMapper();

  private HdfsDataSegmentPusher hdfsDataSegmentPusher;
  @Before
  public void setUp() throws IOException
  {
    HdfsDataSegmentPusherConfig hdfsDataSegmentPusherConf = new HdfsDataSegmentPusherConfig();
    hdfsDataSegmentPusherConf.setStorageDirectory("path/to/");
    hdfsDataSegmentPusher = new HdfsDataSegmentPusher(hdfsDataSegmentPusherConf, new Configuration(true), objectMapper);
  }
  static {
    objectMapper = new TestObjectMapper();
    InjectableValues.Std injectableValues = new InjectableValues.Std();
    injectableValues.addValue(ObjectMapper.class, objectMapper);
    injectableValues.addValue(DataSegment.PruneLoadSpecHolder.class, DataSegment.PruneLoadSpecHolder.DEFAULT);
    objectMapper.setInjectableValues(injectableValues);
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
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        NoneShardSpec.instance(),
        0,
        size
    );

    DataSegment segment = pusher.push(segmentDir, segmentToPush, true);


    String indexUri = StringUtils.format(
        "%s/%s/%d_index.zip",
        FileSystem.newInstance(conf).makeQualified(new Path(config.getStorageDirectory())).toUri().toString(),
        pusher.getStorageDir(segmentToPush),
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
    final String segmentPath = pusher.getStorageDir(segment);

    File indexFile = new File(StringUtils.format(
        "%s/%s/%d_index.zip",
        storageDirectory,
        segmentPath,
        segment.getShardSpec().getPartitionNum()
    ));
    Assert.assertTrue(indexFile.exists());
    File descriptorFile = new File(StringUtils.format(
        "%s/%s/%d_descriptor.json",
        storageDirectory,
        segmentPath,
        segment.getShardSpec().getPartitionNum()
    ));
    Assert.assertTrue(descriptorFile.exists());

    // push twice will fail and temp dir cleaned
    File outDir = new File(StringUtils.format("%s/%s", config.getStorageDirectory(), segmentPath));
    outDir.setReadOnly();
    try {
      pusher.push(segmentDir, segmentToPush, true);
    }
    catch (IOException e) {
      Assert.fail("should not throw exception");
    }
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
          Maps.<String, Object>newHashMap(),
          Lists.<String>newArrayList(),
          Lists.<String>newArrayList(),
          new NumberedShardSpec(i, i),
          0,
          size
      );
    }

    for (int i = 0; i < numberOfSegments; i++) {
      final DataSegment pushedSegment = pusher.push(segmentDir, segments[i], true);

      String indexUri = StringUtils.format(
          "%s/%s/%d_index.zip",
          FileSystem.newInstance(conf).makeQualified(new Path(config.getStorageDirectory())).toUri().toString(),
          pusher.getStorageDir(segments[i]),
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
      String segmentPath = pusher.getStorageDir(pushedSegment);

      File indexFile = new File(StringUtils.format(
          "%s/%s/%d_index.zip",
          storageDirectory,
          segmentPath,
          pushedSegment.getShardSpec().getPartitionNum()
      ));
      Assert.assertTrue(indexFile.exists());
      File descriptorFile = new File(StringUtils.format(
          "%s/%s/%d_descriptor.json",
          storageDirectory,
          segmentPath,
          pushedSegment.getShardSpec().getPartitionNum()
      ));
      Assert.assertTrue(descriptorFile.exists());

      //read actual data from descriptor file.
      DataSegment fromDescriptorFileDataSegment = objectMapper.readValue(descriptorFile, DataSegment.class);

      Assert.assertEquals(segments[i].getSize(), pushedSegment.getSize());
      Assert.assertEquals(segments[i], pushedSegment);
      Assert.assertEquals(ImmutableMap.of(
          "type",
          "hdfs",
          "path",
          indexUri
      ), fromDescriptorFileDataSegment.getLoadSpec());
      // rename directory after push
      segmentPath = pusher.getStorageDir(fromDescriptorFileDataSegment);

      indexFile = new File(StringUtils.format(
          "%s/%s/%d_index.zip",
          storageDirectory,
          segmentPath,
          fromDescriptorFileDataSegment.getShardSpec().getPartitionNum()
      ));
      Assert.assertTrue(indexFile.exists());


      // push twice will fail and temp dir cleaned
      File outDir = new File(StringUtils.format("%s/%s", config.getStorageDirectory(), segmentPath));
      outDir.setReadOnly();
      try {
        pusher.push(segmentDir, segments[i], true);
      }
      catch (IOException e) {
        Assert.fail("should not throw exception");
      }
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
            Interval.class, new StdDeserializer<Interval>(Interval.class)
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
    ImmutableMap<String, Object> loadSpec = ImmutableMap.<String, Object>of("something", "or_other");

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

    String storageDir = hdfsDataSegmentPusher.getStorageDir(segment);
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
      throw Throwables.propagate(e);
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

    path = JobHelper.makeFileNamePath(
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
        JobHelper.DESCRIPTOR_JSON,
        hdfsDataSegmentPusher
    );
    Assert.assertEquals(
        "hdfs://server:9100/tmp/druid/datatest/source/20120710T050000.000Z_20120710T060000.000Z/some_brand_new_version"
        + "/4712_descriptor.json",
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
      throw Throwables.propagate(e);
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
        new LocalDataSegmentPusher(new LocalDataSegmentPusherConfig(), objectMapper)
    );
    Assert.assertEquals(
        "file:/tmp/dru:id/data:test/the:data:source/2012-07-10T05:00:00.000Z_2012-07-10T06:00:00.000Z/some:brand:new:"
        + "version/4712/index.zip",
        path.toString()
    );

    path = JobHelper.makeFileNamePath(
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
        JobHelper.DESCRIPTOR_JSON,
        new LocalDataSegmentPusher(new LocalDataSegmentPusherConfig(), objectMapper)
    );
    Assert.assertEquals(
        "file:/tmp/dru:id/data:test/the:data:source/2012-07-10T05:00:00.000Z_2012-07-10T06:00:00.000Z/some:brand:new:"
        + "version/4712/descriptor.json",
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
        new LocalDataSegmentPusher(new LocalDataSegmentPusherConfig(), objectMapper)
    );
    Assert.assertEquals(
        "file:/tmp/dru:id/data:test/the:data:source/2012-07-10T05:00:00.000Z_2012-07-10T06:00:00.000Z/some:brand:new:"
        + "version/4712/index.zip.0",
        path.toString()
    );

  }
}
