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
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

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
    metadataUpdateSpec = mock(MetadataStorageUpdaterJobSpec.class);
    ioConfig = mock(HadoopIOConfig.class);
    spec = mock(HadoopIngestionSpec.class);
    config = mock(HadoopDruidIndexerConfig.class);
    handler = mock(MetadataStorageUpdaterJobHandler.class);

    try (MockedStatic<IndexGeneratorJob> mockedStatic = Mockito.mockStatic(IndexGeneratorJob.class)) {
      mockedStatic.when(() -> IndexGeneratorJob.getPublishedSegmentAndIndexZipFilePaths(config))
                  .thenReturn(DATA_SEGMENT_AND_INDEX_ZIP_FILE_PATHS);


      when(metadataUpdateSpec.getSegmentTable()).thenReturn(SEGMENT_TABLE);
      when(ioConfig.getMetadataUpdateSpec()).thenReturn(metadataUpdateSpec);
      when(spec.getIOConfig()).thenReturn(ioConfig);
      when(config.getSchema()).thenReturn(spec);


      target = new MetadataStorageUpdaterJob(config, handler);

      target.run();

      verify(handler).publishSegments(
          SEGMENT_TABLE,
          DATA_SEGMENT_AND_INDEX_ZIP_FILE_PATHS.stream().map(s -> s.getSegment()).collect(
              Collectors.toList()), HadoopDruidIndexerConfig.JSON_MAPPER
      );

      verify(metadataUpdateSpec).getSegmentTable();
      verify(ioConfig).getMetadataUpdateSpec();
      verify(spec).getIOConfig();
      verify(config).getSchema();
      mockedStatic.verify(() -> IndexGeneratorJob.getPublishedSegmentAndIndexZipFilePaths(config));
      verifyNoMoreInteractions(handler, metadataUpdateSpec, ioConfig, spec, config);
    }
  }
}
