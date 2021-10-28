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

import com.google.common.collect.ImmutableList;
import org.apache.druid.indexer.updater.MetadataStorageUpdaterJobSpec;
import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;
import java.util.stream.Collectors;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
    IndexGeneratorJob.class
})
@PowerMockIgnore({"javax.net.ssl.*"})
@SuppressStaticInitializationFor({
    "org.apache.druid.indexer.HadoopIngestionSpec",
    "org.apache.druid.indexer.HadoopDruidIndexerConfig",
    "org.apache.druid.indexer.IndexGeneratorJob"
})
public class MetadataStorageUpdaterJobTest
{
  private static final List<DataSegmentAndIndexZipFilePath> DATA_SEGMENT_AND_INDEX_ZIP_FILE_PATHS = ImmutableList.of(
      new DataSegmentAndIndexZipFilePath(null, null, null)
  );
  private static final String SEGMENT_TABLE = "segments";
  private HadoopIngestionSpec spec;
  private HadoopIOConfig ioConfig;
  private MetadataStorageUpdaterJobSpec metadataUpdateSpec;
  private HadoopDruidIndexerConfig config;
  private MetadataStorageUpdaterJobHandler handler;
  private MetadataStorageUpdaterJob target;

  @Test
  public void test_run()
  {
    metadataUpdateSpec = PowerMock.createMock(MetadataStorageUpdaterJobSpec.class);
    ioConfig = PowerMock.createMock(HadoopIOConfig.class);
    spec = PowerMock.createMock(HadoopIngestionSpec.class);
    config = PowerMock.createMock(HadoopDruidIndexerConfig.class);
    handler = PowerMock.createMock(MetadataStorageUpdaterJobHandler.class);
    PowerMock.mockStaticNice(IndexGeneratorJob.class);

    EasyMock.expect(metadataUpdateSpec.getSegmentTable()).andReturn(SEGMENT_TABLE);
    EasyMock.expect(ioConfig.getMetadataUpdateSpec()).andReturn(metadataUpdateSpec);
    EasyMock.expect(spec.getIOConfig()).andReturn(ioConfig);
    EasyMock.expect(config.getSchema()).andReturn(spec);
    EasyMock.expect(IndexGeneratorJob.getPublishedSegmentAndIndexZipFilePaths(config))
            .andReturn(DATA_SEGMENT_AND_INDEX_ZIP_FILE_PATHS);
    handler.publishSegments(
        SEGMENT_TABLE,
        DATA_SEGMENT_AND_INDEX_ZIP_FILE_PATHS.stream().map(s -> s.getSegment()).collect(
        Collectors.toList()), HadoopDruidIndexerConfig.JSON_MAPPER);
    EasyMock.expectLastCall();
    target = new MetadataStorageUpdaterJob(config, handler);

    PowerMock.replayAll();

    target.run();

    PowerMock.verifyAll();
  }
}
