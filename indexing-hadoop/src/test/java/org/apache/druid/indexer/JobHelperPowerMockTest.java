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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.timeline.DataSegment;
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
    FileSystemHelper.class,
    HadoopDruidIndexerConfig.class
})
@PowerMockIgnore({"javax.net.ssl.*"})
public class JobHelperPowerMockTest
{
  private static final String TMP_PATH = "/tmp/index.zip.0";
  private static final String FINAL_PATH = "/final/index.zip.0";

  private HadoopDruidIndexerConfig indexerConfig;

  @Test
  public void test_renameIndexFilesForSegments_emptySegments() throws IOException
  {
    HadoopIngestionSpec ingestionSpec = mockIngestionSpec();
    List<DataSegmentAndIndexZipFilePath> segmentAndIndexZipFilePaths = ImmutableList.of();

    PowerMock.replayAll();

    JobHelper.renameIndexFilesForSegments(ingestionSpec, segmentAndIndexZipFilePaths);

    PowerMock.verifyAll();
  }

  @Test
  public void test_renameIndexFilesForSegments_segmentIndexFileRenamedSuccessfully()
      throws IOException
  {
    HadoopIngestionSpec ingestionSpec = mockIngestionSpec();
    mockFileSystem(true);
    DataSegment segment = PowerMock.createMock(DataSegment.class);

    List<DataSegmentAndIndexZipFilePath> segmentAndIndexZipFilePaths = ImmutableList.of(
        new DataSegmentAndIndexZipFilePath(
            segment,
            TMP_PATH,
            FINAL_PATH
        )
    );
    PowerMock.replayAll();

    JobHelper.renameIndexFilesForSegments(ingestionSpec, segmentAndIndexZipFilePaths);

    PowerMock.verifyAll();
  }

  @Test (expected = IOE.class)
  public void test_renameIndexFilesForSegments_segmentIndexFileRenamedFailed_throwsException()
      throws IOException
  {
    HadoopIngestionSpec ingestionSpec = mockIngestionSpec();
    mockFileSystem(false);
    DataSegment segment = PowerMock.createMock(DataSegment.class);
    List<DataSegmentAndIndexZipFilePath> segmentAndIndexZipFilePaths = ImmutableList.of(
        new DataSegmentAndIndexZipFilePath(
            segment,
            TMP_PATH,
            FINAL_PATH
        )
    );

    PowerMock.replayAll();

    JobHelper.renameIndexFilesForSegments(ingestionSpec, segmentAndIndexZipFilePaths);

    PowerMock.verifyAll();
  }

  @Test
  public void test_maybeDeleteIntermediatePath_leaveIntermediate_doesNotDeleteIntermediatePath()
  {
    HadoopIngestionSpec ingestionSpec = mockIngestionSpec();
    HadoopTuningConfig tuningConfig = PowerMock.createMock(HadoopTuningConfig.class);
    EasyMock.expect(tuningConfig.isLeaveIntermediate()).andReturn(true);
    EasyMock.expect(ingestionSpec.getTuningConfig()).andReturn(tuningConfig);

    PowerMock.replayAll();

    JobHelper.maybeDeleteIntermediatePath(true, ingestionSpec);

    PowerMock.verifyAll();
  }

  @Test
  public void test_maybeDeleteIntermediatePath_doNotleaveIntermediateAndIndexerJobSucceeded_deleteIntermediatePath()
      throws IOException
  {
    HadoopIngestionSpec ingestionSpec = mockIngestionSpec();
    HadoopTuningConfig tuningConfig = PowerMock.createMock(HadoopTuningConfig.class);
    Path workingPath = PowerMock.createMock(Path.class);
    FileSystem workingPathFs = PowerMock.createMock(FileSystem.class);
    EasyMock.expect(tuningConfig.isLeaveIntermediate()).andReturn(false);
    EasyMock.expect(ingestionSpec.getTuningConfig()).andReturn(tuningConfig);
    EasyMock.expect(workingPathFs.delete(workingPath, true)).andReturn(true);
    EasyMock.expect(workingPath.getFileSystem(EasyMock.anyObject())).andReturn(workingPathFs);
    EasyMock.expect(indexerConfig.makeIntermediatePath()).andReturn(workingPath);

    PowerMock.replayAll();

    JobHelper.maybeDeleteIntermediatePath(true, ingestionSpec);

    PowerMock.verifyAll();
  }

  @Test
  public void test_maybeDeleteIntermediatePath_doNotleaveIntermediateAndIndexJobFailedAndCleanupOnFailure_deleteIntermediatePath()
      throws IOException
  {
    HadoopIngestionSpec ingestionSpec = mockIngestionSpec();
    HadoopTuningConfig tuningConfig = PowerMock.createMock(HadoopTuningConfig.class);
    Path workingPath = PowerMock.createMock(Path.class);
    FileSystem workingPathFs = PowerMock.createMock(FileSystem.class);
    EasyMock.expect(tuningConfig.isLeaveIntermediate()).andReturn(false);
    EasyMock.expect(tuningConfig.isCleanupOnFailure()).andReturn(true);
    EasyMock.expect(ingestionSpec.getTuningConfig()).andReturn(tuningConfig).anyTimes();
    EasyMock.expect(workingPathFs.delete(workingPath, true)).andReturn(true);
    EasyMock.expect(workingPath.getFileSystem(EasyMock.anyObject())).andReturn(workingPathFs);
    EasyMock.expect(indexerConfig.makeIntermediatePath()).andReturn(workingPath);

    PowerMock.replayAll();

    JobHelper.maybeDeleteIntermediatePath(false, ingestionSpec);

    PowerMock.verifyAll();
  }

  @Test
  public void test_maybeDeleteIntermediatePath_deleteThrowsException_noExceptionPropogated()
      throws IOException
  {
    HadoopIngestionSpec ingestionSpec = mockIngestionSpec();
    HadoopTuningConfig tuningConfig = PowerMock.createMock(HadoopTuningConfig.class);
    Path workingPath = PowerMock.createMock(Path.class);
    FileSystem workingPathFs = PowerMock.createMock(FileSystem.class);
    EasyMock.expect(tuningConfig.isLeaveIntermediate()).andReturn(false);
    EasyMock.expect(tuningConfig.isCleanupOnFailure()).andReturn(true);
    EasyMock.expect(ingestionSpec.getTuningConfig()).andReturn(tuningConfig).anyTimes();
    EasyMock.expect(workingPathFs.delete(workingPath, true)).andThrow(new IOException("Delete Exception"));
    EasyMock.expect(workingPath.getFileSystem(EasyMock.anyObject())).andReturn(workingPathFs);
    EasyMock.expect(indexerConfig.makeIntermediatePath()).andReturn(workingPath);

    PowerMock.replayAll();

    JobHelper.maybeDeleteIntermediatePath(false, ingestionSpec);

    PowerMock.verifyAll();
  }

  private HadoopIngestionSpec mockIngestionSpec()
  {
    indexerConfig = PowerMock.createMock(HadoopDruidIndexerConfig.class);
    HadoopIngestionSpec ingestionSpec = PowerMock.createMock(HadoopIngestionSpec.class);
    PowerMock.mockStaticNice(HadoopDruidIndexerConfig.class);
    EasyMock.expect(indexerConfig.getAllowedProperties()).andReturn(ImmutableMap.of()).anyTimes();
    indexerConfig.addJobProperties(EasyMock.anyObject(Configuration.class));
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(HadoopDruidIndexerConfig.fromSpec(ingestionSpec)).andReturn(indexerConfig);
    EasyMock.expect(indexerConfig.getSchema()).andReturn(ingestionSpec).anyTimes();
    return ingestionSpec;
  }

  private void mockFileSystem(boolean renameSuccess) throws IOException
  {
    PowerMock.mockStaticNice(FileSystemHelper.class);
    FileSystem fileSystem = PowerMock.createMock(FileSystem.class);
    EasyMock.expect(FileSystemHelper.get(
        EasyMock.anyObject(URI.class),
        EasyMock.anyObject(Configuration.class)
    )).andReturn(fileSystem);
    EasyMock.expect(fileSystem.exists(EasyMock.anyObject(Path.class))).andReturn(false);
    EasyMock.expect(fileSystem.rename(EasyMock.anyObject(Path.class), EasyMock.anyObject(Path.class)))
            .andReturn(renameSuccess);
  }
}
