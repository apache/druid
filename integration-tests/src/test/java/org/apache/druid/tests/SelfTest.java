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

package org.apache.druid.tests;

import com.google.inject.Inject;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.testing.clients.QueryResourceTestClient;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.guice.TestClient;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

@Guice(moduleFactory = SelfTest.LocalModuleFactory.class)
public class SelfTest
{

  static class LocalModuleFactory extends DruidTestModuleFactory
  {

  }

  @TestClient
  HttpClient httpClient;

  @Inject
  private QueryResourceTestClient queryClient;

  @Test
  public void asd()
  {
    queryClient.query("httpfds", null);
    // que
  }

}
