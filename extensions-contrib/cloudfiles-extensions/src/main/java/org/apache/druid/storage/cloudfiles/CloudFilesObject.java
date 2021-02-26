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

import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import org.jclouds.io.Payload;
import org.jclouds.io.Payloads;

import java.io.File;

public class CloudFilesObject
{

  private Payload payload;
  private String path;
  private final String region;
  private final String container;

  public CloudFilesObject(final String basePath, final File file, final String region, final String container)
  {
    this(region, container);
    ByteSource byteSource = Files.asByteSource(file);
    this.payload = Payloads.newByteSourcePayload(byteSource);
    this.path = CloudFilesUtils.buildCloudFilesPath(basePath, file.getName());
  }

  public CloudFilesObject(final Payload payload, final String region, final String container, final String path)
  {
    this(region, container, path);
    this.payload = payload;
  }

  private CloudFilesObject(final String region, final String container, final String path)
  {
    this(region, container);
    this.path = path;
  }

  private CloudFilesObject(final String region, final String container)
  {
    this.region = region;
    this.container = container;
  }

  public String getRegion()
  {
    return region;
  }

  public String getContainer()
  {
    return container;
  }

  public String getPath()
  {
    return path;
  }

  public Payload getPayload()
  {
    return payload;
  }

}
