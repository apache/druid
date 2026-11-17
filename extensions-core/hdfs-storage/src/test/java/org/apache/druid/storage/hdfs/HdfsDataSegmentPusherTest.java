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
import org.apache.druid.jackson.GranularityModule;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.loading.DeepStorageSegmentConfig;
import org.apache.druid.testing.TemporaryFolderExtension;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.DataSegment.PruneSpecsHolder;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.utils.CompressionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

  @RegisterExtension
  public final TemporaryFolderExtension tempFolder = TemporaryFolderExtension.testCaseScoped();

  private HdfsDataSegmentPusher hdfsDataSegmentPusher;

  @BeforeEach
  public void setUp()
  {
    HdfsDataSegmentPusherConfig hdfsDataSegmentPusherConf = new HdfsDataSegmentPusherConfig();
    hdfsDataSegmentPusherConf.setStorageDirectory("path/to/");
    hdfsDataSegmentPusher = new HdfsDataSegmentPusher(hdfsDataSegmentPusherConf, new Configuration(true));
  }

  @Test
  public void testPushWithScheme() throws Exception
  {
    testUsingScheme("file");
  }

  @Test
  public void testPushWithBadScheme()
  {
    Throwable exception = assertThrows(RuntimeException.class, () -> {
      testUsingScheme("xyzzy");

      // Not reached
      Assertions.assertTrue(false);
    });
    assertTrue(exception.getMessage().contains("No FileSystem for scheme"));
  }

  @Test
  public void testPushWithoutScheme() throws Exception
  {
    testUsingScheme(null);
  }

  @Test
  public void testPushWithMultipleSegments() throws Exception
  {
    testUsingSchemeForMultipleSegments("file", 3, CompressionUtils.Format.ZIP);
    testUsingSchemeForMultipleSegments("file", 3, CompressionUtils.Format.LZ4);
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
    HdfsDataSegmentPusher pusher = new HdfsDataSegmentPusher(config, conf);

    DataSegment segmentToPush = DataSegment.builder(SegmentId.of("foo", Intervals.of("2015/2016"), "0", 0))
                                           .loadSpec(new HashMap<>())
                                           .dimensions(new ArrayList<>())
                                           .metrics(new ArrayList<>())
                                           .shardSpec(NoneShardSpec.instance())
                                           .binaryVersion(0)
                                           .size(size)
                                           .build();

    DataSegment segment = pusher.push(segmentDir, segmentToPush, true);

    Pattern pattern =
        Pattern.compile(".*/foo/20150101T000000\\.000Z_20160101T000000\\.000Z/0/0_[A-Za-z0-9-]{36}_index\\.zip");
    Assertions.assertTrue(
        pattern.matcher(segment.getLoadSpec().get("path").toString()).matches(),
        segment.getLoadSpec().get("path").toString()
    );
  }

  @Test
  public void testPushToPath() throws Exception
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
    HdfsDataSegmentPusher pusher = new HdfsDataSegmentPusher(config, conf);

    DataSegment segmentToPush = DataSegment.builder(SegmentId.of("foo", Intervals.of("2015/2016"), "0", 0))
                                           .loadSpec(new HashMap<>())
                                           .dimensions(new ArrayList<>())
                                           .metrics(new ArrayList<>())
                                           .shardSpec(NoneShardSpec.instance())
                                           .binaryVersion(0)
                                           .size(size)
                                           .build();

    final String storageDirSuffix = "shuffle-data/index_parallel_session_analysis_test_bkdhhedd_2023-09-08T03:18:21.121Z/2023-08-29T16:00:00.000Z/2023-08-29T17:00:00.000Z";
    DataSegment segment = pusher.pushToPath(segmentDir, segmentToPush, storageDirSuffix);
    
    Assertions.assertTrue(
        segment.getLoadSpec().get("path").toString().endsWith(storageDirSuffix.replace(':', '_') + "/0_index.zip"),
        segment.getLoadSpec().get("path").toString()
    );
  }

  private void testUsingSchemeForMultipleSegments(final String scheme, final int numberOfSegments, CompressionUtils.Format format) throws Exception
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

    HdfsDataSegmentPusher pusher = new HdfsDataSegmentPusher(
        config,
        conf,
        null,
        new DeepStorageSegmentConfig(null, format)
    );

    for (int i = 0; i < numberOfSegments; i++) {
      segments[i] = DataSegment.builder(SegmentId.of("foo", Intervals.of("2015/2016"), "0", i))
                               .loadSpec(new HashMap<>())
                               .dimensions(new ArrayList<>())
                               .metrics(new ArrayList<>())
                               .shardSpec(new NumberedShardSpec(i, i))
                               .binaryVersion(0)
                               .size(size)
                               .build();
    }

    for (int i = 0; i < numberOfSegments; i++) {
      final DataSegment pushedSegment = pusher.push(segmentDir, segments[i], false);

      final String indexUri;
      try (FileSystem fileSystem = FileSystem.newInstance(conf)) {
        indexUri = StringUtils.format(
            "%s/%s/%d_index.%s",
            fileSystem.makeQualified(new Path(config.getStorageDirectory())).toUri().toString(),
            pusher.getStorageDir(segments[i], false),
            segments[i].getShardSpec().getPartitionNum(),
            format.getExtension()
        );
      }

      Assertions.assertEquals(segments[i].getSize(), pushedSegment.getSize());
      Assertions.assertEquals(segments[i], pushedSegment);
      Assertions.assertEquals(ImmutableMap.of(
          "type",
          "hdfs",
          "path",
          indexUri
      ), pushedSegment.getLoadSpec());
      // rename directory after push
      String segmentPath = pusher.getStorageDir(pushedSegment, false);

      File indexFile = new File(StringUtils.format(
          "%s/%s/%d_index.%s",
          storageDirectory,
          segmentPath,
          pushedSegment.getShardSpec().getPartitionNum(),
          format.getExtension()
      ));
      Assertions.assertTrue(indexFile.exists());

      Assertions.assertEquals(segments[i].getSize(), pushedSegment.getSize());
      Assertions.assertEquals(segments[i], pushedSegment);

      indexFile = new File(StringUtils.format(
          "%s/%s/%d_index.%s",
          storageDirectory,
          segmentPath,
          pushedSegment.getShardSpec().getPartitionNum(),
          format.getExtension()
      ));
      Assertions.assertTrue(indexFile.exists());


      // push twice will fail and temp dir cleaned
      final File outDir = new File(storageDirectory, segmentPath);
      Assertions.assertTrue(outDir.setReadOnly(), "test setup must be able to make the output directory read-only");
      try {
        pusher.push(segmentDir, segments[i], false);
      }
      catch (IOException e) {
        Assertions.fail("should not throw exception");
      }
      finally {
        Assertions.assertTrue(outDir.setWritable(true), "test teardown must restore write permission");
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
    HdfsDataSegmentPusher pusher = new HdfsDataSegmentPusher(config, conf);

    DataSegment segmentToPush = DataSegment.builder(SegmentId.of("foo", Intervals.of("2015/2016"), "0", 0))
                                           .loadSpec(new HashMap<>())
                                           .dimensions(new ArrayList<>())
                                           .metrics(new ArrayList<>())
                                           .shardSpec(NoneShardSpec.instance())
                                           .binaryVersion(0)
                                           .size(size)
                                           .build();

    DataSegment segment = pusher.push(segmentDir, segmentToPush, false);


    final String indexUri;
    try (FileSystem fileSystem = FileSystem.newInstance(conf)) {
      indexUri = StringUtils.format(
          "%s/%s/%d_index.zip",
          fileSystem.makeQualified(new Path(config.getStorageDirectory())).toUri().toString(),
          pusher.getStorageDir(segmentToPush, false),
          segmentToPush.getShardSpec().getPartitionNum()
      );
    }

    Assertions.assertEquals(segmentToPush.getSize(), segment.getSize());
    Assertions.assertEquals(segmentToPush, segment);
    Assertions.assertEquals(ImmutableMap.of(
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
    Assertions.assertTrue(indexFile.exists());

    // push twice will fail and temp dir cleaned
    final File outDir = new File(storageDirectory, segmentPath);
    Assertions.assertTrue(outDir.setReadOnly(), "test setup must be able to make the output directory read-only");
    try {
      pusher.push(segmentDir, segmentToPush, false);
    }
    catch (IOException e) {
      Assertions.fail("should not throw exception");
    }
    finally {
      Assertions.assertTrue(outDir.setWritable(true), "test teardown must restore write permission");
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
            new StdDeserializer<>(Interval.class)
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

    DataSegment segment = DataSegment.builder(SegmentId.of("something", interval, "brand:new:version", 0))
                                     .loadSpec(loadSpec)
                                     .dimensions(Arrays.asList("dim1", "dim2"))
                                     .metrics(Arrays.asList("met1", "met2"))
                                     .shardSpec(NoneShardSpec.instance())
                                     .binaryVersion(null)
                                     .size(1)
                                     .build();

    String storageDir = hdfsDataSegmentPusher.getStorageDir(segment, false);
    Assertions.assertEquals("something/20111001T000000.000Z_20111002T000000.000Z/brand_new_version", storageDir);

  }

  @Test
  public void testPushNoneDirectorySegment() throws Exception
  {
    final Configuration conf = new Configuration(true);

    final File segmentDir = tempFolder.newFolder();
    final File tmp1 = new File(segmentDir, "version.bin");
    final File tmp2 = new File(segmentDir, "index.smoosh");
    final byte[] data1 = new byte[]{0x0, 0x0, 0x0, 0x1};
    final byte[] data2 = new byte[]{0x1, 0x2, 0x3, 0x4, 0x5};
    Files.write(data1, tmp1);
    Files.write(data2, tmp2);
    final long expectedSize = data1.length + data2.length;

    final HdfsDataSegmentPusherConfig config = new HdfsDataSegmentPusherConfig();
    final File storageDirectory = tempFolder.newFolder();
    config.setStorageDirectory(storageDirectory.getAbsolutePath());

    final HdfsDataSegmentPusher pusher = new HdfsDataSegmentPusher(
        config,
        conf,
        null,
        new DeepStorageSegmentConfig(null, CompressionUtils.Format.NONE)
    );

    final DataSegment segmentToPush = DataSegment.builder(SegmentId.of("foo", Intervals.of("2015/2016"), "0", 0))
                                                 .loadSpec(new HashMap<>())
                                                 .dimensions(new ArrayList<>())
                                                 .metrics(new ArrayList<>())
                                                 .shardSpec(NoneShardSpec.instance())
                                                 .binaryVersion(0)
                                                 .size(expectedSize)
                                                 .build();

    final DataSegment pushedSegment = pusher.push(segmentDir, segmentToPush, false);

    Assertions.assertEquals(expectedSize, pushedSegment.getSize());
    Assertions.assertEquals(segmentToPush, pushedSegment);

    final String pathUri = pushedSegment.getLoadSpec().get("path").toString();
    Assertions.assertTrue(pathUri.endsWith("/0_index"), pathUri);

    final File segmentOutDir = new File(
        StringUtils.format(
            "%s/%s/%d_index",
            storageDirectory,
            pusher.getStorageDir(pushedSegment, false),
            pushedSegment.getShardSpec().getPartitionNum()
        )
    );
    Assertions.assertTrue(segmentOutDir.exists());
    Assertions.assertTrue(segmentOutDir.isDirectory());
    Assertions.assertTrue(new File(segmentOutDir, "version.bin").exists());
    Assertions.assertTrue(new File(segmentOutDir, "index.smoosh").exists());

    // Pull directory segment and verify contents
    final HdfsDataSegmentPuller puller = new HdfsDataSegmentPuller(conf);
    final File pullTargetDir = tempFolder.newFolder();
    puller.getSegmentFiles(new Path(pathUri), pullTargetDir);
    Assertions.assertTrue(new File(pullTargetDir, "version.bin").exists());
    Assertions.assertTrue(new File(pullTargetDir, "index.smoosh").exists());
    Assertions.assertArrayEquals(data1, Files.toByteArray(new File(pullTargetDir, "version.bin")));
    Assertions.assertArrayEquals(data2, Files.toByteArray(new File(pullTargetDir, "index.smoosh")));
  }

  @Test
  public void testPushNoneWithUniquePath() throws Exception
  {
    final Configuration conf = new Configuration(true);

    final File segmentDir = tempFolder.newFolder();
    final File tmp = new File(segmentDir, "version.bin");
    final byte[] data = new byte[]{0x0, 0x0, 0x0, 0x1};
    Files.write(data, tmp);

    final HdfsDataSegmentPusherConfig config = new HdfsDataSegmentPusherConfig();
    final File storageDirectory = tempFolder.newFolder();
    config.setStorageDirectory(storageDirectory.getAbsolutePath());

    final HdfsDataSegmentPusher pusher = new HdfsDataSegmentPusher(
        config,
        conf,
        null,
        new DeepStorageSegmentConfig(null, CompressionUtils.Format.NONE)
    );

    final DataSegment segmentToPush = DataSegment.builder(SegmentId.of("foo", Intervals.of("2015/2016"), "0", 0))
                                                 .loadSpec(new HashMap<>())
                                                 .dimensions(new ArrayList<>())
                                                 .metrics(new ArrayList<>())
                                                 .shardSpec(NoneShardSpec.instance())
                                                 .binaryVersion(0)
                                                 .size(data.length)
                                                 .build();

    final DataSegment pushedSegment = pusher.push(segmentDir, segmentToPush, true);

    final Pattern pattern =
        Pattern.compile(".*/foo/20150101T000000\\.000Z_20160101T000000\\.000Z/0/0_[A-Za-z0-9-]{36}_index");
    Assertions.assertTrue(
        pattern.matcher(pushedSegment.getLoadSpec().get("path").toString()).matches(),
        pushedSegment.getLoadSpec().get("path").toString()
    );
  }

  @Test
  public void testPushUnsupportedFormatThrows()
  {
    final HdfsDataSegmentPusherConfig config = new HdfsDataSegmentPusherConfig();
    config.setStorageDirectory("/tmp");
    final Configuration conf = new Configuration(true);

    Assertions.assertThrows(
        IAE.class,
        () -> new HdfsDataSegmentPusher(
            config,
            conf,
            null,
            new DeepStorageSegmentConfig(null, CompressionUtils.Format.SNAPPY)
        )
    );
  }

  @Test
  public void testPushToPathWithNoneAndDirectorySuffixEndingInIndex() throws Exception
  {
    final Configuration conf = new Configuration(true);
    final File segmentDir = tempFolder.newFolder();
    Files.write(new byte[]{0x0, 0x0, 0x0, 0x9}, new File(segmentDir, "version.bin"));

    final HdfsDataSegmentPusherConfig config = new HdfsDataSegmentPusherConfig();
    final File storageDirectory = tempFolder.newFolder();
    config.setStorageDirectory(storageDirectory.getAbsolutePath());

    final HdfsDataSegmentPusher pusher = new HdfsDataSegmentPusher(
      config,
      conf,
      null,
      new DeepStorageSegmentConfig(null, CompressionUtils.Format.NONE)
    );

    final DataSegment segment = DataSegment.builder(SegmentId.of("foo", Intervals.of("2015/2016"), "0", 0))
      .loadSpec(new HashMap<>())
      .dimensions(new ArrayList<>())
      .metrics(new ArrayList<>())
      .shardSpec(new NumberedShardSpec(0, 0))
      .binaryVersion(0)
      .size(3)
      .build();

    final String suffix = "some-directory/index";
    final DataSegment pushed = pusher.pushToPath(segmentDir, segment, suffix);

    final String path = Objects.requireNonNull(pushed.getLoadSpec()).get("path").toString();
    Assertions.assertTrue(path.endsWith("some-directory/index/0_index"), path);
    Assertions.assertTrue(new File(URI.create(path).getPath()).isDirectory());
  }

  @Test
  public void testPushNoneRejectsInvalidInputFile() throws Exception
  {
    final HdfsDataSegmentPusherConfig config = new HdfsDataSegmentPusherConfig();
    config.setStorageDirectory(tempFolder.newFolder().getAbsolutePath());

    final HdfsDataSegmentPusher pusher = new HdfsDataSegmentPusher(
      config,
      new Configuration(true),
      null,
      new DeepStorageSegmentConfig(null, CompressionUtils.Format.ZIP)
    );

    final DataSegment segment = DataSegment.builder(
        SegmentId.of("foo", Intervals.of("2015/2016"), "0", 0)
      )
      .loadSpec(new HashMap<>())
      .dimensions(new ArrayList<>())
      .metrics(new ArrayList<>())
      .shardSpec(new NumberedShardSpec(0, 0))
      .binaryVersion(0)
      .size(0)
      .build();

    final File invalidSegmentDir = tempFolder.newFile();

    Assertions.assertThrows(
      Exception.class,
      () -> pusher.push(invalidSegmentDir, segment, false),
      "Pushing a regular file instead of a segment directory should fail early"
    );
  }

  @Test
  public void testPushV9ThenV10RetainsZipRepresentation() throws Exception
  {
    assertRepresentationTransition(
      CompressionUtils.Format.ZIP,
      IndexIO.V9_VERSION,
      IndexIO.V10_VERSION
    );
  }

  @Test
  public void testPushV10ThenV9RetainsZipRepresentation() throws Exception
  {
    assertRepresentationTransition(
      CompressionUtils.Format.ZIP,
      IndexIO.V10_VERSION,
      IndexIO.V9_VERSION
    );
  }

  @Test
  public void testPushV9ThenV10RetainsLz4Representation() throws Exception
  {
    assertRepresentationTransition(
      CompressionUtils.Format.LZ4,
      IndexIO.V9_VERSION,
      IndexIO.V10_VERSION
    );
  }

  @Test
  public void testPushV10ThenV9RetainsLz4Representation() throws Exception
  {
    assertRepresentationTransition(
      CompressionUtils.Format.LZ4,
      IndexIO.V10_VERSION,
      IndexIO.V9_VERSION
    );
  }

  @Test
  public void testV10PushEmitsNoneFormatMetricWhenZipIsConfigured() throws Exception
  {
    assertPushMetricFormat(
      IndexIO.V10_VERSION,
      CompressionUtils.Format.NONE
    );
  }

  @Test
  public void testV9PushEmitsConfiguredZipFormatMetric() throws Exception
  {
    assertPushMetricFormat(
      IndexIO.V9_VERSION,
      CompressionUtils.Format.ZIP
    );
  }

  private void assertPushMetricFormat(
    int segmentVersion,
    CompressionUtils.Format expectedFormat
  ) throws Exception
  {
    final List<ServiceMetricEvent> metricEvents = new ArrayList<>();

    final ServiceEmitter serviceEmitter = createCapturingServiceEmitter(metricEvents);

    final HdfsDataSegmentPusherConfig config = new HdfsDataSegmentPusherConfig();
    config.setStorageDirectory(tempFolder.newFolder().getAbsolutePath());

    final HdfsDataSegmentPusher pusher = new HdfsDataSegmentPusher(
      config,
      new Configuration(true),
      serviceEmitter,
      new DeepStorageSegmentConfig(null, CompressionUtils.Format.ZIP)
    );

    final DataSegment segment = DataSegment.builder(
        SegmentId.of("foo", Intervals.of("2015/2016"), "0", 0)
      )
      .loadSpec(new HashMap<>())
      .dimensions(new ArrayList<>())
      .metrics(new ArrayList<>())
      .shardSpec(new NumberedShardSpec(0, 0))
      .binaryVersion(0)
      .size(0)
      .build();

    pusher.push(createSegmentDirectory(segmentVersion), segment, false);

    Assertions.assertEquals(2, metricEvents.size());

    assertMetricFormat(
      metricEvents,
      "hdfs/push/size",
      expectedFormat
    );

    assertMetricFormat(
      metricEvents,
      "hdfs/push/duration",
      expectedFormat
    );
  }

  private ServiceEmitter createCapturingServiceEmitter(List<ServiceMetricEvent> metricEvents)
  {
    final Emitter capturingEmitter = new Emitter()
    {
      @Override
      public void start()
      {
      }

      @Override
      public void emit(Event event)
      {
        if (event instanceof ServiceMetricEvent) {
          metricEvents.add((ServiceMetricEvent) event);
        }
      }

      @Override
      public void flush()
      {
      }

      @Override
      public void close()
      {
      }
    };

    return new ServiceEmitter(
      "test",
      "localhost",
      capturingEmitter
    );
  }

  private void assertMetricFormat(
    List<ServiceMetricEvent> metricEvents,
    String metricName,
    CompressionUtils.Format expectedFormat
  )
  {
    final ServiceMetricEvent metricEvent = metricEvents.stream()
      .filter(event -> metricName.equals(event.getMetric()))
      .findFirst()
      .orElseThrow(() -> new AssertionError("Missing metric: " + metricName));

    Assertions.assertEquals(
      expectedFormat.toString(),
      Objects.requireNonNull(metricEvent.getUserDims().get("format")).toString(),
      "Incorrect format dimension for metric " + metricName
    );
  }

  private void assertRepresentationTransition(
    CompressionUtils.Format compressionFormat,
    int firstVersion,
    int secondVersion
  ) throws Exception
  {
    final Configuration conf = new Configuration(true);

    final HdfsDataSegmentPusherConfig config = new HdfsDataSegmentPusherConfig();
    final File storageDirectory = tempFolder.newFolder();
    config.setStorageDirectory(storageDirectory.getAbsolutePath());

    final HdfsDataSegmentPusher pusher = new HdfsDataSegmentPusher(
      config,
      conf,
      null,
      new DeepStorageSegmentConfig(null, compressionFormat)
    );

    final DataSegment segment = DataSegment.builder(
        SegmentId.of("foo", Intervals.of("2015/2016"), "0", 0)
      )
      .loadSpec(new HashMap<>())
      .dimensions(new ArrayList<>())
      .metrics(new ArrayList<>())
      .shardSpec(new NumberedShardSpec(0, 0))
      .binaryVersion(0)
      .size(0)
      .build();

    final File segmentParent = new File(
      storageDirectory,
      pusher.getStorageDir(segment, false)
    );

    final File zipPath = new File(segmentParent, "0_index.zip");
    final File lz4Path = new File(segmentParent, "0_index.lz4");
    final File directoryPath = new File(segmentParent, "0_index");

    final File firstSegmentDir = createSegmentDirectory(firstVersion);
    final DataSegment firstPush = pusher.push(firstSegmentDir, segment, false);

    final File firstRepresentation = firstVersion == IndexIO.V10_VERSION
      ? directoryPath
      : compressionFormat == CompressionUtils.Format.ZIP ? zipPath : lz4Path;

    Assertions.assertTrue(
      firstRepresentation.exists(),
      "The first representation should exist after the initial push"
    );

    Assertions.assertTrue(
      new File(URI.create(Objects.requireNonNull(firstPush.getLoadSpec()).get("path").toString())).exists(),
      "The first loadSpec path should exist after the initial push"
    );

    verifySegmentContents(firstRepresentation);

    final File secondSegmentDir = createSegmentDirectory(secondVersion);
    final DataSegment secondPush = pusher.push(secondSegmentDir, segment, false);

    final File secondRepresentation = secondVersion == IndexIO.V10_VERSION
      ? directoryPath
      : compressionFormat == CompressionUtils.Format.ZIP ? zipPath : lz4Path;

    Assertions.assertTrue(
      firstRepresentation.exists(),
      "The previous representation must not be deleted after the second push"
    );

    Assertions.assertTrue(
      secondRepresentation.exists(),
      "The new representation should exist after the second push"
    );

    Assertions.assertTrue(
      new File(URI.create(firstPush.getLoadSpec().get("path").toString())).exists(),
      "The first push's loadSpec path must remain valid"
    );

    Assertions.assertTrue(
      new File(URI.create(Objects.requireNonNull(secondPush.getLoadSpec()).get("path").toString())).exists(),
      "The second push's loadSpec path must exist"
    );

    verifySegmentContents(firstRepresentation);
    verifySegmentContents(secondRepresentation);

    Assertions.assertEquals(firstVersion, firstPush.getBinaryVersion());
    Assertions.assertEquals(secondVersion, secondPush.getBinaryVersion());

    Assertions.assertNotEquals(
      firstPush.getLoadSpec().get("path"),
      secondPush.getLoadSpec().get("path"),
      "Different segment representations should have different loadSpec paths"
    );

    Assertions.assertEquals(
      firstVersion == IndexIO.V10_VERSION || secondVersion == IndexIO.V10_VERSION,
      directoryPath.isDirectory()
    );
  }

  private void verifySegmentContents(File path) throws IOException
  {
    if (path.isDirectory()) {
      Assertions.assertTrue(new File(path, "version.bin").exists(), "version.bin must exist in directory");
      Assertions.assertTrue(new File(path, "index.smoosh").exists(), "index.smoosh must exist in directory");
    } else if (path.getName().endsWith(".zip")) {
      final File unzipped = tempFolder.newFolder();
      CompressionUtils.unzip(path, unzipped);
      Assertions.assertTrue(new File(unzipped, "version.bin").exists(), "version.bin must exist in unzipped archive");
      Assertions.assertTrue(new File(unzipped, "index.smoosh").exists(), "index.smoosh must exist in unzipped archive");
    } else {
      Assertions.assertTrue(path.isFile(), "Representation must be a file");
      Assertions.assertTrue(path.length() > 0, "Compressed file must not be empty");
    }
  }

  @Test
  public void testConcurrentV9AndV10PushesDoNotDeleteEachOther() throws Exception
  {
    final HdfsDataSegmentPusherConfig config = new HdfsDataSegmentPusherConfig();
    config.setStorageDirectory(tempFolder.newFolder().getAbsolutePath());

    final HdfsDataSegmentPusher pusher1 = new HdfsDataSegmentPusher(
      config,
      new Configuration(true),
      null,
      new DeepStorageSegmentConfig(null, CompressionUtils.Format.ZIP)
    );

    final HdfsDataSegmentPusher pusher2 = new HdfsDataSegmentPusher(
      config,
      new Configuration(true),
      null,
      new DeepStorageSegmentConfig(null, CompressionUtils.Format.ZIP)
    );

    final DataSegment segmentToPush = DataSegment.builder(
        SegmentId.of("foo", Intervals.of("2015/2016"), "0", 0)
      )
      .loadSpec(new HashMap<>())
      .dimensions(new ArrayList<>())
      .metrics(new ArrayList<>())
      .shardSpec(new NumberedShardSpec(0, 0))
      .binaryVersion(0)
      .size(0)
      .build();

    final File v9SegmentDir = createSegmentDirectory(IndexIO.V9_VERSION);
    final File v10SegmentDir = createSegmentDirectory(IndexIO.V10_VERSION);
    final ExecutorService executor = Execs.multiThreaded(2, "HdfsDataSegmentPusherTest-%d");
    final CountDownLatch startLatch = new CountDownLatch(1);

    try {
      Future<DataSegment> v9Future = executor.submit(() -> {
        startLatch.await();
        return pusher1.push(v9SegmentDir, segmentToPush, false);
      });

      Future<DataSegment> v10Future = executor.submit(() -> {
        startLatch.await();
        return pusher2.push(v10SegmentDir, segmentToPush, false);
      });

      startLatch.countDown();

      final DataSegment v9PushedSegment = v9Future.get();
      final DataSegment v10PushedSegment = v10Future.get();

      final File v9Path = new File(URI.create(Objects.requireNonNull(v9PushedSegment.getLoadSpec()).get("path").toString()));
      final File v10Path = new File(URI.create(Objects.requireNonNull(v10PushedSegment.getLoadSpec()).get("path").toString()));

      Assertions.assertTrue(
        v9Path.exists(),
        "V9 zip file should exist after concurrent push"
      );
      Assertions.assertTrue(
        v9Path.isFile(),
        "V9 ZIP representation must be a file"
      );
      Assertions.assertTrue(
        v10Path.exists(),
        "V10 directory should exist after concurrent push"
      );
      Assertions.assertTrue(
        v10Path.isDirectory(),
        "V10 representation must be a directory"
      );
    }
    finally {
      executor.shutdownNow();
    }
  }

  private File createSegmentDirectory(int version) throws IOException
  {
    final File segmentDir = tempFolder.newFolder();

    final File versionFile = new File(segmentDir, "version.bin");
    Files.write(
      ByteBuffer.allocate(Integer.BYTES).putInt(version).array(),
      versionFile
    );

    Files.write(
      new byte[]{0x1, 0x2, 0x3, 0x4},
      new File(segmentDir, "index.smoosh")
    );

    return segmentDir;
  }
}
