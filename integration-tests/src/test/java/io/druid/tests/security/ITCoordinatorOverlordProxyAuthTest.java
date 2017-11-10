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

package io.druid.tests.security;

import com.google.inject.Inject;
import io.druid.testing.clients.CoordinatorResourceTestClient;
import io.druid.testing.guice.DruidTestModuleFactory;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.testng.Assert;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITCoordinatorOverlordProxyAuthTest
{
  @Inject
  CoordinatorResourceTestClient coordinatorClient;
  
  @Test
  public void testProxyAuth() throws Exception
  {
    HttpResponseStatus responseStatus = coordinatorClient.getProxiedOverlordScalingResponseStatus();
    Assert.assertEquals(HttpResponseStatus.OK, responseStatus);
  }
}
