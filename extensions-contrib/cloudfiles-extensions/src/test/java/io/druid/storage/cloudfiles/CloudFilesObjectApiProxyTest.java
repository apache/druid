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

package io.druid.storage.cloudfiles;

import org.easymock.EasyMockSupport;
import org.jclouds.io.Payload;
import org.jclouds.openstack.swift.v1.domain.SwiftObject;
import org.jclouds.openstack.swift.v1.features.ObjectApi;
import org.jclouds.rackspace.cloudfiles.v1.CloudFilesApi;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class CloudFilesObjectApiProxyTest extends EasyMockSupport
{

  @Test
  public void getTest()
  {
    final String path = "path";
    final String region = "region";
    final String container = "container";

    CloudFilesApi cloudFilesApi = createMock(CloudFilesApi.class);
    ObjectApi objectApi = createMock(ObjectApi.class);
    SwiftObject swiftObject = createMock(SwiftObject.class);
    Payload payload = createMock(Payload.class);

    expect(cloudFilesApi.getObjectApi(region, container)).andReturn(objectApi);
    expect(objectApi.get(path)).andReturn(swiftObject);
    expect(swiftObject.getPayload()).andReturn(payload);

    replayAll();

    CloudFilesObjectApiProxy cfoApiProxy = new CloudFilesObjectApiProxy(cloudFilesApi, region, container);
    CloudFilesObject cloudFilesObject = cfoApiProxy.get(path);

    assertEquals(cloudFilesObject.getPayload(), payload);
    assertEquals(cloudFilesObject.getRegion(), region);
    assertEquals(cloudFilesObject.getContainer(), container);
    assertEquals(cloudFilesObject.getPath(), path);

    verifyAll();
  }

}
