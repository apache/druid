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

package org.apache.druid.quidem;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LauncherSmokeTest
{
  private static Launcher launcher;

  @BeforeClass
  public static void setUp() throws Exception
  {
    launcher = new Launcher("druidtest:///");
    launcher.start();
  }

  @AfterClass
  public static void tearDown()
  {
    launcher.shutdown();
  }

  @Test
  public void chkSelectFromFoo() throws Exception
  {
    CloseableHttpClient client = HttpClients.createDefault();
    HttpPost request = new HttpPost("http://localhost:12345/druid/v2/sql");
    request.addHeader("Content-Type", "application/json");
    request.setEntity(new StringEntity("{\"query\":\"Select * from foo\"}"));
    CloseableHttpResponse response = client.execute(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void chkStatusWorks() throws Exception
  {
    CloseableHttpClient client = HttpClients.createDefault();
    HttpGet request = new HttpGet("http://localhost:12345/status");
    request.addHeader("Content-Type", "application/json");
    CloseableHttpResponse response = client.execute(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
    String responseStr = EntityUtils.toString(response.getEntity());
    MatcherAssert.assertThat(responseStr, Matchers.containsString("\"version\":\""));
  }
}
