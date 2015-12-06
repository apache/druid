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

package io.druid.indexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobContext;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
public class HadoopIOPeonTest
{
  final String TMP_FILE_NAME = "test_file";
  JobContext mockJobContext;
  Configuration jobConfig;
  boolean overwritesFiles = true;
  HadoopIOPeon ioPeon;

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before public void setUp() throws IOException
  {
    jobConfig = new Configuration();
    mockJobContext = EasyMock.createMock(JobContext.class);
    EasyMock.expect(mockJobContext.getConfiguration()).andReturn(jobConfig).anyTimes();
    EasyMock.replay(mockJobContext);

    ioPeon = new HadoopIOPeon(mockJobContext,new Path(tmpFolder.newFile().getParent()),overwritesFiles);
  }

  @After public void tearDown()
  {
    jobConfig = null;
    mockJobContext = null;
    tmpFolder.delete();
  }

  @Test public void testMakeOutputStream() throws IOException
  {
    Assert.assertNotNull(ioPeon.makeOutputStream(TMP_FILE_NAME));
  }

  @Test public void testMakeInputStream() throws IOException
  {
    Assert.assertNotNull(ioPeon.makeInputStream(tmpFolder.newFile(TMP_FILE_NAME).getName()));
  }

  @Test(expected = UnsupportedOperationException.class) public void testCleanup() throws IOException
  {
    ioPeon.cleanup();
  }
}
