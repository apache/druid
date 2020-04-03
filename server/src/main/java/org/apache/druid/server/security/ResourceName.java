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

package org.apache.druid.server.security;

import com.fasterxml.jackson.annotation.JsonCreator;

public class ResourceName
{
  private final String resourceName;

  public static final ResourceName COMPACTION = new ResourceName("COMPACTION");
  public static final ResourceName CONFIG = new ResourceName("CONFIG");
  public static final ResourceName INTERNAL = new ResourceName("INTERNAL");
  public static final ResourceName LOOKUP = new ResourceName("LOOKUP");
  public static final ResourceName READINESS = new ResourceName("READINESS");
  public static final ResourceName RULES = new ResourceName("RULES");
  public static final ResourceName SAMPLER = new ResourceName("SAMPLER");
  public static final ResourceName SECURITY = new ResourceName("SECURITY");
  public static final ResourceName SERVERS = new ResourceName("SERVERS");
  public static final ResourceName STATUS = new ResourceName("STATUS");
  public static final ResourceName SUPERUSER = new ResourceName(".*");
  public static final ResourceName WORKER = new ResourceName("WORKER");

  public ResourceName(String name)
  {
    this.resourceName = name;
  }

  @JsonCreator
  public static ResourceName fromString(String name)
  {
    if (name == null) {
      return null;
    }
    return new ResourceName(name);
  }

  @Override
  public String toString()
  {
    return this.resourceName;
  }

}
