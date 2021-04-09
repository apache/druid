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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.path.StaticPathSpec;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.URI;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
    FileSystemHelper.class
})
@PowerMockIgnore({"javax.net.ssl.*"})
public class JobHelperPowerMockTest
{
  private static final DataSegment DATA_SEGMENT = new DataSegment(
      "test1",
      Intervals.of("2000/3000"),
      "ver",
      ImmutableMap.of(
          "type", "google",
          "bucket", "test-test",
          "path", "tmp/foo:bar/index1.zip"
      ),
      ImmutableList.of(),
      ImmutableList.of(),
      NoneShardSpec.instance(),
      9,
      1024
  );
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();
  private static final DataSchema DATA_SCHEMA = new DataSchema(
      "test_ds",
      JSON_MAPPER.convertValue(
          new HadoopyStringInputRowParser(
              new JSONParseSpec(
                  new TimestampSpec("t", "auto", null),
                  new DimensionsSpec(
                      DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim1t", "dim2")),
                      null,
                      null
                  ),
                  new JSONPathSpec(true, ImmutableList.of()),
                  ImmutableMap.of(),
                  null
              )
          ),
          JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
      ),
      new AggregatorFactory[]{new CountAggregatorFactory("rows")},
      new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, null),
      null,
      JSON_MAPPER
  );

  private static final HadoopIOConfig IO_CONFIG = new HadoopIOConfig(
      JSON_MAPPER.convertValue(
          new StaticPathSpec("dummyPath", null),
          JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
      ),
      null,
      "dummyOutputPath"
  );

  private static final HadoopTuningConfig TUNING_CONFIG = HadoopTuningConfig
      .makeDefaultTuningConfig()
      .withWorkingPath("dummyWorkingPath");
  private static final HadoopIngestionSpec DUMMY_SPEC = new HadoopIngestionSpec(DATA_SCHEMA, IO_CONFIG, TUNING_CONFIG);

  @Test
  public void test_renameIndexFilesForSegments_emptySegments() throws IOException
  {
    List<DataSegmentAndIndexZipFilePath> segmentAndIndexZipFilePaths = ImmutableList.of();
    JobHelper.renameIndexFilesForSegments(DUMMY_SPEC, segmentAndIndexZipFilePaths);
  }

  @Test
  public void test_renameIndexFilesForSegments_segmentIndexFileRenamedSuccessfully()
      throws IOException
  {
    String tmpPath = "/tmp/index.zip.0";
    String finalPath = "/final/index.zip.0";
    List<DataSegmentAndIndexZipFilePath> segmentAndIndexZipFilePaths = ImmutableList.of(
        new DataSegmentAndIndexZipFilePath(
            DATA_SEGMENT,
            tmpPath,
            finalPath
        )
    );
    PowerMock.mockStaticNice(FileSystemHelper.class);
    FileSystem fileSystem = PowerMock.createMock(FileSystem.class);
    EasyMock.expect(FileSystemHelper.get(
        EasyMock.anyObject(URI.class),
        EasyMock.anyObject(Configuration.class)
    )).andReturn(fileSystem);
    EasyMock.expect(fileSystem.exists(EasyMock.anyObject(Path.class))).andReturn(false);
    EasyMock.expect(fileSystem.rename(EasyMock.anyObject(Path.class), EasyMock.anyObject(Path.class))).andReturn(true);

    PowerMock.replayAll();

    JobHelper.renameIndexFilesForSegments(DUMMY_SPEC, segmentAndIndexZipFilePaths);

    PowerMock.verifyAll();
  }

  @Test (expected = IOE.class)
  public void test_renameIndexFilesForSegments_segmentIndexFileRenamedFailed_throwsException()
      throws IOException
  {
    String tmpPath = "/tmp/index.zip.0";
    String finalPath = "/final/index.zip.0";
    List<DataSegmentAndIndexZipFilePath> segmentAndIndexZipFilePaths = ImmutableList.of(
        new DataSegmentAndIndexZipFilePath(
            DATA_SEGMENT,
            tmpPath,
            finalPath
        )
    );
    PowerMock.mockStaticNice(FileSystemHelper.class);
    FileSystem fileSystem = PowerMock.createMock(FileSystem.class);
    EasyMock.expect(FileSystemHelper.get(
        EasyMock.anyObject(URI.class),
        EasyMock.anyObject(Configuration.class)
    )).andReturn(fileSystem);
    EasyMock.expect(fileSystem.exists(EasyMock.anyObject(Path.class))).andReturn(false);
    EasyMock.expect(fileSystem.rename(EasyMock.anyObject(Path.class), EasyMock.anyObject(Path.class))).andReturn(false);

    PowerMock.replayAll();

    JobHelper.renameIndexFilesForSegments(DUMMY_SPEC, segmentAndIndexZipFilePaths);

    PowerMock.verifyAll();
  }
}
