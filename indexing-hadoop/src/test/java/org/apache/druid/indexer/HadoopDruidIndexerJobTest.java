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

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
    JobHelper.class,
    IndexGeneratorJob.class
})
@PowerMockIgnore({"javax.net.ssl.*", "org.apache.logging.log4j.*"})
@SuppressStaticInitializationFor({
    "org.apache.druid.indexer.HadoopDruidIndexerConfig",
    "org.apache.druid.indexer.JobHelper",
    "org.apache.druid.indexer.IndexGeneratorJob"
})
public class HadoopDruidIndexerJobTest
{
  private HadoopDruidIndexerConfig config;
  private MetadataStorageUpdaterJobHandler handler;
  private HadoopDruidIndexerJob target;

  @Test
  public void test_run()
  {
    config = PowerMock.createMock(HadoopDruidIndexerConfig.class);
    handler = PowerMock.createMock(MetadataStorageUpdaterJobHandler.class);
    PowerMock.mockStaticNice(JobHelper.class);
    PowerMock.mockStaticNice(IndexGeneratorJob.class);
    config.verify();
    EasyMock.expectLastCall();
    EasyMock.expect(config.isUpdaterJobSpecSet()).andReturn(false).anyTimes();
    config.setHadoopJobIdFileName(EasyMock.anyString());
    EasyMock.expectLastCall();
    JobHelper.ensurePaths(config);
    EasyMock.expectLastCall();
    Capture<List<Jobby>> capturedJobs = Capture.newInstance();
    EasyMock.expect(JobHelper.runJobs(EasyMock.capture(capturedJobs))).andReturn(true);
    EasyMock.expect(IndexGeneratorJob.getPublishedSegmentAndIndexZipFilePaths(EasyMock.anyObject())).andReturn(null);


    PowerMock.replayAll();

    target = new HadoopDruidIndexerJob(config, handler);
    target.run();

    List<Jobby> jobs = capturedJobs.getValue();
    Assert.assertEquals(2, jobs.size());
    jobs.stream().filter(job -> !(job instanceof IndexGeneratorJob)).forEach(job -> Assert.assertTrue(job.run()));

    PowerMock.verifyAll();
  }
}
