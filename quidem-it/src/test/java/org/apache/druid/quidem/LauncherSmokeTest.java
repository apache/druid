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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
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
  public void chkSelectFromFoo() throws IOException, InterruptedException
  {
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:12345/druid/v2/sql"))
        .header("Content-Type", "application/json")
        .POST(BodyPublishers.ofString("{\"query\":\"Select * from foo\"}"))
        .build();
    HttpClient hc = HttpClient.newHttpClient();
    HttpResponse<String> a = hc.send(request, HttpResponse.BodyHandlers.ofString());
    assertEquals(200, a.statusCode());
  }

  @Test
  public void chkStatusWorks() throws IOException, InterruptedException
  {
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:12345/status"))
        .header("Content-Type", "application/json")
        .GET()
        .build();
    HttpClient hc = HttpClient.newHttpClient();
    HttpResponse<String> a = hc.send(request, HttpResponse.BodyHandlers.ofString());
    assertEquals(200, a.statusCode());
  }
}
