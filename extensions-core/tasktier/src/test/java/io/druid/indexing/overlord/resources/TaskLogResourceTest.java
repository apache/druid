/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexing.overlord.resources;

import com.google.common.io.ByteStreams;
import com.metamx.common.StringUtils;
import io.druid.indexing.overlord.TierLocalTaskRunner;
import io.druid.server.DruidNode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;

public class TaskLogResourceTest
{
  private static final File logFile = new File(TierLocalTaskRunner.LOG_FILE_NAME);

  @Before
  public void setUp()
  {
    deleteLogFile();
  }

  @After
  public void tearDown()
  {
    deleteLogFile();
  }

  private void deleteLogFile()
  {
    Assert.assertTrue("cannot cleanup log file", (!logFile.exists() || logFile.delete()) || (!logFile.exists()));
  }

  @Test
  public void testGetNoLog() throws Exception
  {
    final TaskLogResource taskLogResource = new TaskLogResource();
    Assert.assertFalse(logFile.exists());
    final Response response = taskLogResource.getLog(0);
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }


  @Test
  public void testGetEmptyLog() throws Exception
  {
    final TaskLogResource taskLogResource = new TaskLogResource();
    Assert.assertFalse(logFile.exists());
    Assert.assertTrue(logFile.createNewFile());
    final Response response = taskLogResource.getLog(0);
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    final Object entity = response.getEntity();
    Assert.assertTrue(entity instanceof InputStream);
    try (InputStream fis = (InputStream) entity) {
      Assert.assertEquals(0, fis.available());
    }
  }

  @Test
  public void testNegativeOffset() throws Exception
  {
    final TaskLogResource taskLogResource = new TaskLogResource();
    Assert.assertFalse(logFile.exists());
    Assert.assertTrue(logFile.createNewFile());
    final Response response = taskLogResource.getLog(-1);
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
  }

  @Test
  public void testLogWithOffset() throws Exception
  {
    final String string = "some test string for logging";
    final byte[] data = StringUtils.toUtf8(string);
    try (final FileOutputStream fos = new FileOutputStream(logFile)) {
      fos.write(data);
    }

    final TaskLogResource taskLogResource = new TaskLogResource();
    Assert.assertTrue(logFile.exists());
    for (int i = 0; i < data.length; ++i) {
      final Response response = taskLogResource.getLog(i);
      Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
      final Object entity = response.getEntity();
      final ByteArrayOutputStream baos = new ByteArrayOutputStream(data.length - i);
      try (InputStream fis = (InputStream) entity) {
        ByteStreams.copy(fis, baos);
      }
      Assert.assertEquals(string.substring(i), StringUtils.fromUtf8(baos.toByteArray()));
    }
  }

  @Test
  public void testLogPastEnd() throws Exception
  {
    final TaskLogResource taskLogResource = new TaskLogResource();
    Assert.assertFalse(logFile.exists());
    Assert.assertTrue(logFile.createNewFile());
    for (int i = 1; i < 100; ++i) {
      final Response response = taskLogResource.getLog(i);
      Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
      final Object entity = response.getEntity();
      Assert.assertTrue(entity instanceof InputStream);
      try (InputStream fis = (InputStream) entity) {
        Assert.assertEquals(0, fis.available());
      }
    }
  }

  @Test
  public void testBuildURL() throws Exception
  {
    final DruidNode testNode = new DruidNode("testService", "somehost", 1234);
    for (int i = 0; i < 10000; ++i) {
      final URL url = TaskLogResource.buildURL(testNode, i);
      Assert.assertEquals("http", url.getProtocol());
      Assert.assertEquals(testNode.getHost(), url.getHost());
      Assert.assertEquals(testNode.getPort(), url.getPort());
      Assert.assertEquals(String.format("%s=%d", TaskLogResource.OFFSET_PARAM, i), url.getQuery());
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuildURLBadOffset() throws Exception
  {
    final DruidNode testNode = new DruidNode("testService", "somehost", 1234);
    TaskLogResource.buildURL(testNode, -1);
  }
}

