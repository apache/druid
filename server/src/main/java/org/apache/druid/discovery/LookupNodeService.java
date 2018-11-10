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

package org.apache.druid.discovery;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Metadata announced by any node that serves queries and hence applies lookups.
 */
public class LookupNodeService extends DruidService
{
  public static final String DISCOVERY_SERVICE_KEY = "lookupNodeService";

  private final String lookupTier;

  public LookupNodeService(
      @JsonProperty("lookupTier") String lookupTier
  )
  {
    this.lookupTier = lookupTier;
  }

  @Override
  public String getName()
  {
    return DISCOVERY_SERVICE_KEY;
  }

  @JsonProperty
  public String getLookupTier()
  {
    return lookupTier;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LookupNodeService that = (LookupNodeService) o;
    return Objects.equals(lookupTier, that.lookupTier);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(lookupTier);
  }

  @Override
  public String toString()
  {
    return "LookupNodeService{" +
           "lookupTier='" + lookupTier + '\'' +
           '}';
  }
}
