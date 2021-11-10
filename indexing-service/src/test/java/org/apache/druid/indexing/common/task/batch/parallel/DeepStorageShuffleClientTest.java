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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.apache.druid.guice.GuiceAnnotationIntrospector;
import org.apache.druid.guice.GuiceInjectableValues;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.loading.LocalDataSegmentPuller;
import org.apache.druid.segment.loading.LocalLoadSpec;
import org.apache.druid.utils.CompressionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

public class DeepStorageShuffleClientTest
{
  private DeepStorageShuffleClient deepStorageShuffleClient;
  private ObjectMapper mapper;
  private File segmentFile;
  private String segmentFileName;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();


  @Before
  public void setUp() throws Exception
  {
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
    deepStorageShuffleClient = new DeepStorageShuffleClient(mapper);

    File temp = temporaryFolder.newFile();
    segmentFileName = temp.getName();
    try (Writer writer = Files.newBufferedWriter(temp.toPath(), StandardCharsets.UTF_8)) {
      for (int j = 0; j < 10; j++) {
        writer.write(StringUtils.format("let's write some data.\n"));
      }
    }
    segmentFile = new File(temp.getAbsolutePath() + ".zip");
    CompressionUtils.zip(segmentFile.getParentFile(), segmentFile);
  }

  @Test
  public void fetchSegmentFile() throws IOException
  {
    File partitionDir = temporaryFolder.newFolder();
    String subTaskId = "subTask";
    File unzippedDir = deepStorageShuffleClient.fetchSegmentFile(
        partitionDir,
        "testSupervisor",
        new DeepStoragePartitionLocation(
            subTaskId,
            Intervals.of("2000/2099"),
            null,
            ImmutableMap.of("type", "local", "path", segmentFile.getAbsolutePath())
        )
    );
    Assert.assertEquals(
        StringUtils.format("%s/unzipped_%s", partitionDir.getAbsolutePath(), subTaskId),
        unzippedDir.getAbsolutePath()
    );
    File fetchedSegmentFile = unzippedDir.listFiles((dir, name) -> name.endsWith(".tmp"))[0];
    Assert.assertEquals(segmentFileName, fetchedSegmentFile.getName());
    Assert.assertTrue(fetchedSegmentFile.length() > 0);
  }
}
