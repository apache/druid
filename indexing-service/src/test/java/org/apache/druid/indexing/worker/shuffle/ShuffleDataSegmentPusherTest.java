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

package org.apache.druid.indexing.worker.shuffle;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import com.google.inject.Injector;
import org.apache.commons.io.FileUtils;
import org.apache.druid.client.indexing.NoopOverlordClient;
import org.apache.druid.guice.GuiceAnnotationIntrospector;
import org.apache.druid.guice.GuiceInjectableValues;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.loading.LoadSpec;
import org.apache.druid.segment.loading.LocalDataSegmentPuller;
import org.apache.druid.segment.loading.LocalDataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.druid.segment.loading.LocalLoadSpec;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.BucketNumberedShardSpec;
import org.apache.druid.utils.CompressionUtils;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

@RunWith(Parameterized.class)
public class ShuffleDataSegmentPusherTest
{
  private static final String LOCAL = "local";
  private static final String DEEPSTORE = "deepstore";

  @Parameterized.Parameters(name = "intermediateDataManager={0}")
  public static Collection<Object[]> data()
  {
    return ImmutableList.of(new Object[]{LOCAL}, new Object[]{DEEPSTORE});
  }

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private IntermediaryDataManager intermediaryDataManager;
  private ShuffleDataSegmentPusher segmentPusher;
  private ObjectMapper mapper;

  private final String intermediateDataStore;
  private File localDeepStore;

  public ShuffleDataSegmentPusherTest(String intermediateDataStore)
  {
    this.intermediateDataStore = intermediateDataStore;
  }

  @Before
  public void setup() throws IOException
  {
    final WorkerConfig workerConfig = new WorkerConfig();
    final TaskConfig taskConfig = new TaskConfig(
        null,
        null,
        null,
        null,
        null,
        false,
        null,
        null,
        ImmutableList.of(new StorageLocationConfig(temporaryFolder.newFolder(), null, null)),
        false,
        false,
        TaskConfig.BATCH_PROCESSING_MODE_DEFAULT.name(),
        null
    );
    final OverlordClient overlordClient = new NoopOverlordClient();
    if (LOCAL.equals(intermediateDataStore)) {
      intermediaryDataManager = new LocalIntermediaryDataManager(workerConfig, taskConfig, overlordClient);
    } else if (DEEPSTORE.equals(intermediateDataStore)) {
      localDeepStore = temporaryFolder.newFolder("localStorage");
      intermediaryDataManager = new DeepStorageIntermediaryDataManager(
          new LocalDataSegmentPusher(
              new LocalDataSegmentPusherConfig()
              {
                @Override
                public File getStorageDirectory()
                {
                  return localDeepStore;
                }
              }));
    }
    intermediaryDataManager.start();
    segmentPusher = new ShuffleDataSegmentPusher("supervisorTaskId", "subTaskId", intermediaryDataManager);

    final Injector injector = GuiceInjectors.makeStartupInjectorWithModules(
        ImmutableList.of(
            binder -> binder.bind(LocalDataSegmentPuller.class)
        )
    );
    mapper = new DefaultObjectMapper();
    mapper.registerModule(new SimpleModule("loadSpecTest").registerSubtypes(LocalLoadSpec.class));
    mapper.setInjectableValues(new GuiceInjectableValues(injector));
    final GuiceAnnotationIntrospector guiceIntrospector = new GuiceAnnotationIntrospector();
    mapper.setAnnotationIntrospectors(
        new AnnotationIntrospectorPair(guiceIntrospector, mapper.getSerializationConfig().getAnnotationIntrospector()),
        new AnnotationIntrospectorPair(guiceIntrospector, mapper.getDeserializationConfig().getAnnotationIntrospector())
    );
  }

  @After
  public void teardown()
  {
    intermediaryDataManager.stop();
  }

  @Test
  public void testPush() throws IOException, SegmentLoadingException
  {
    final File segmentDir = generateSegmentDir();
    final DataSegment segment = newSegment(Intervals.of("2018/2019"));
    final DataSegment pushed = segmentPusher.push(segmentDir, segment, true);

    Assert.assertEquals(9, pushed.getBinaryVersion().intValue());
    Assert.assertEquals(14, pushed.getSize()); // 10 bytes data + 4 bytes version

    final File tempDir = temporaryFolder.newFolder();
    if (intermediaryDataManager instanceof LocalIntermediaryDataManager) {
      final Optional<ByteSource> zippedSegment = intermediaryDataManager.findPartitionFile(
          "supervisorTaskId",
          "subTaskId",
          segment.getInterval(),
          segment.getShardSpec().getPartitionNum()
      );
      Assert.assertTrue(zippedSegment.isPresent());
      CompressionUtils.unzip(
          zippedSegment.get(),
          tempDir,
          org.apache.druid.java.util.common.FileUtils.IS_EXCEPTION,
          false
      );
    } else if (intermediaryDataManager instanceof DeepStorageIntermediaryDataManager) {
      final LoadSpec loadSpec = mapper.convertValue(pushed.getLoadSpec(), LoadSpec.class);
      Assert.assertTrue(pushed.getLoadSpec()
                              .get("path")
                              .toString()
                              .startsWith(localDeepStore.getAbsolutePath()
                                          + "/"
                                          + DeepStorageIntermediaryDataManager.SHUFFLE_DATA_DIR_PREFIX));
      loadSpec.loadSegment(tempDir);
    }

    final List<File> unzippedFiles = Arrays.asList(tempDir.listFiles());
    unzippedFiles.sort(Comparator.comparing(File::getName));
    final File dataFile = unzippedFiles.get(0);
    Assert.assertEquals("test", dataFile.getName());
    Assert.assertEquals("test data.", Files.readFirstLine(dataFile, StandardCharsets.UTF_8));
    final File versionFile = unzippedFiles.get(1);
    Assert.assertEquals("version.bin", versionFile.getName());
    Assert.assertArrayEquals(Ints.toByteArray(0x9), Files.toByteArray(versionFile));
  }

  private File generateSegmentDir() throws IOException
  {
    // Each file size is 138 bytes after compression
    final File segmentDir = temporaryFolder.newFolder();
    Files.asByteSink(new File(segmentDir, "version.bin")).write(Ints.toByteArray(0x9));
    FileUtils.write(new File(segmentDir, "test"), "test data.", StandardCharsets.UTF_8);
    return segmentDir;
  }

  private DataSegment newSegment(Interval interval)
  {
    BucketNumberedShardSpec<?> shardSpec = Mockito.mock(BucketNumberedShardSpec.class);
    Mockito.when(shardSpec.getBucketId()).thenReturn(0);

    return new DataSegment(
        "dataSource",
        interval,
        "version",
        null,
        null,
        null,
        shardSpec,
        9,
        0
    );
  }
}
