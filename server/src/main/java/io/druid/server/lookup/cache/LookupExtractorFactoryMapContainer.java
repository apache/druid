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

package io.druid.server.lookup.cache;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.java.util.common.guava.Comparators;

import java.util.Map;
import java.util.Objects;

/**
 * This is same as LookupExtractorFactoryContainer except it uses Map<String, Object> instead of
 * LookupExtractorFactory for referencing lookup spec so that lookup extensions are not required to
 * be loaded at the Coordinator.
 */
public class LookupExtractorFactoryMapContainer
{
  private final String version;
  private final Map<String, Object> lookupExtractorFactory;

  @JsonCreator
  public LookupExtractorFactoryMapContainer(
      @JsonProperty("version") String version,
      @JsonProperty("lookupExtractorFactory") Map<String, Object> lookupExtractorFactory
  )
  {
    this.version = version;
    this.lookupExtractorFactory = Preconditions.checkNotNull(lookupExtractorFactory, "null factory");
  }

  @JsonProperty
  public String getVersion()
  {
    return version;
  }

  @JsonProperty
  public Map<String, Object> getLookupExtractorFactory()
  {
    return lookupExtractorFactory;
  }

  public boolean replaces(LookupExtractorFactoryMapContainer other)
  {
    if (version == null && other.getVersion() == null) {
      return false;
    }

    return Comparators.<String>naturalNullsFirst().compare(version, other.getVersion()) > 0;
  }

  @Override
  public String toString()
  {
    return "LookupExtractorFactoryContainer{" +
           "version='" + version + '\'' +
           ", lookupExtractorFactory=" + lookupExtractorFactory +
           '}';
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
    LookupExtractorFactoryMapContainer that = (LookupExtractorFactoryMapContainer) o;
    return Objects.equals(version, that.version) &&
           Objects.equals(lookupExtractorFactory, that.lookupExtractorFactory);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(version, lookupExtractorFactory);
  }
}
