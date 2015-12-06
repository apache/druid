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

import org.jclouds.io.Payload;
import org.jclouds.openstack.swift.v1.domain.SwiftObject;
import org.jclouds.openstack.swift.v1.features.ObjectApi;
import org.jclouds.rackspace.cloudfiles.v1.CloudFilesApi;

public class CloudFilesObjectApiProxy
{
  private final ObjectApi objectApi;
  private final String region;
  private final String container;

  public CloudFilesObjectApiProxy(final CloudFilesApi cloudFilesApi, final String region, final String container)
  {
    this.region = region;
    this.container = container;
    this.objectApi = cloudFilesApi.getObjectApi(region, container);
  }

  public String getRegion()
  {
    return region;
  }

  public String getContainer()
  {
    return container;
  }

  public String put(final CloudFilesObject cloudFilesObject)
  {
    return objectApi.put(cloudFilesObject.getPath(), cloudFilesObject.getPayload());
  }

  public CloudFilesObject get(String path)
  {
    SwiftObject swiftObject = objectApi.get(path);
    Payload payload = swiftObject.getPayload();
    return new CloudFilesObject(payload, this.region, this.container, path);
  }
}
