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

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class HadoopDruidIndexerJobTest
{
  @Test
  public void test_run()
  {
    HadoopDruidIndexerConfig config = mock(HadoopDruidIndexerConfig.class);
    MetadataStorageUpdaterJobHandler handler = mock(MetadataStorageUpdaterJobHandler.class);
    try (MockedStatic<JobHelper> jobHelperMock = Mockito.mockStatic(JobHelper.class)) {
      try (final MockedStatic<IndexGeneratorJob> indexGeneratorJobMock = Mockito.mockStatic(IndexGeneratorJob.class)) {
        when(config.isUpdaterJobSpecSet()).thenReturn(false);

        jobHelperMock.when(() -> JobHelper.runJobs(any())).thenReturn(true);
        indexGeneratorJobMock.when(() -> IndexGeneratorJob.getPublishedSegmentAndIndexZipFilePaths(any())).thenReturn(null);

        HadoopDruidIndexerJob target = new HadoopDruidIndexerJob(config, handler);
        target.run();

        ArgumentCaptor<List<Jobby>> capturedJobs = ArgumentCaptor.forClass(List.class);
        jobHelperMock.verify(() -> JobHelper.runJobs(capturedJobs.capture()));

        List<Jobby> jobs = capturedJobs.getValue();
        Assert.assertEquals(2, jobs.size());
        jobs.stream().filter(job -> !(job instanceof IndexGeneratorJob)).forEach(job -> Assert.assertTrue(job.run()));

        jobHelperMock.verify(() -> JobHelper.ensurePaths(config));
        jobHelperMock.verifyNoMoreInteractions();

        indexGeneratorJobMock.verify(() -> IndexGeneratorJob.getPublishedSegmentAndIndexZipFilePaths(any()));
        indexGeneratorJobMock.verifyNoMoreInteractions();

        verify(config).verify();
        verify(config, atLeastOnce()).isUpdaterJobSpecSet();
        verify(config).setHadoopJobIdFileName(null);
        verifyNoMoreInteractions(config);
      }
    }
  }
}
