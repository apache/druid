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

package org.apache.druid.storage.cloudfiles;

import org.jclouds.io.Payload;
import org.jclouds.openstack.swift.v1.domain.SwiftObject;
import org.jclouds.openstack.swift.v1.features.ObjectApi;
import org.jclouds.rackspace.cloudfiles.v1.CloudFilesApi;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CloudFilesObjectApiProxyTest
{
  @Test
  public void getTest()
  {
    final String path = "path";
    final String region = "region";
    final String container = "container";

    CloudFilesApi cloudFilesApi = mock(CloudFilesApi.class);
    ObjectApi objectApi = mock(ObjectApi.class);
    SwiftObject swiftObject = mock(SwiftObject.class);
    Payload payload = mock(Payload.class);

    when(cloudFilesApi.getObjectApi(region, container)).thenReturn(objectApi);
    when(objectApi.get(path)).thenReturn(swiftObject);
    when(swiftObject.getPayload()).thenReturn(payload);

    CloudFilesObjectApiProxy cfoApiProxy = new CloudFilesObjectApiProxy(cloudFilesApi, region, container);
    CloudFilesObject cloudFilesObject = cfoApiProxy.get(path, 0);

    Assert.assertEquals(cloudFilesObject.getPayload(), payload);
    Assert.assertEquals(cloudFilesObject.getRegion(), region);
    Assert.assertEquals(cloudFilesObject.getContainer(), container);
    Assert.assertEquals(cloudFilesObject.getPath(), path);

    verify(cloudFilesApi).getObjectApi(region, container);
    verify(objectApi).get(path);
    verify(swiftObject).getPayload();
  }
}
