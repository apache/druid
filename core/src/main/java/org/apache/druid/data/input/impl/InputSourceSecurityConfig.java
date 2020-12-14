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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class InputSourceSecurityConfig
{

  public static InputSourceSecurityConfig ALLOW_ALL = new InputSourceSecurityConfig(null, null);
  @JsonProperty
  private final List<URI> allowPrefexList;
  @JsonProperty
  private final List<URI> denyPrefixList;

  @JsonCreator
  public InputSourceSecurityConfig(
      @JsonProperty("allowPrefexList") @Nullable List<URI> allowPrefexList,
      @JsonProperty("denyPrefixList") @Nullable List<URI> denyPrefixList
  )
  {
    this.allowPrefexList = allowPrefexList;
    this.denyPrefixList = denyPrefixList;
    Preconditions.checkArgument(
        this.denyPrefixList == null || this.allowPrefexList == null,
        "Can only use one of allowList or blackList"
    );
  }

  public List<URI> getallowPrefexList()
  {
    return allowPrefexList;
  }

  public List<URI> getdenyPrefixList()
  {
    return denyPrefixList;
  }

  private static boolean matchesAny(List<URI> domains, URI uri)
  {
    URI toMatch = uri.normalize();
    for (URI prefix : domains) {
      return toMatch.toString().startsWith(prefix.toString());
    }
    return false;
  }

  public void validateURIAccess(@Nullable Collection<URI> uris)
  {
    if(uris == null){
      return;
    }
    uris.forEach(this::validateURIAccess);
  }

  public void validateURIAccess(@Nullable URI uri)
  {
    if(uri == null){
      return;
    }
    Preconditions.checkArgument(
        isURIAllowed(uri),
        StringUtils.format("Access to [%s] DENIED!", uri)
    );
  }

  public void validateFileAccess(@Nullable File file)
  {
    if(file == null){
      return;
    }
    validateURIAccess(file.toURI());
  }

  public void validateFileAccess(@Nullable Collection<File> files)
  {
    if(files == null){
      return;
    }
    files.forEach(this::validateFileAccess);
  }

  private void validateCloudLocationAccess(@Nullable CloudObjectLocation location, String scheme){
    if(location == null){
      return;
    }
    validateURIAccess(location.toUri(scheme));
  }

  public void validateCloudLocationAccess(@Nullable Collection<CloudObjectLocation> locations, String scheme)
  {
    if(locations == null){
      return;
    }
    locations.forEach(location -> validateCloudLocationAccess(location, scheme));
  }

  public boolean isURIAllowed(URI uri)
  {
    if (allowPrefexList != null) {
      return matchesAny(allowPrefexList, uri);
    }
    if (denyPrefixList != null) {
      return !matchesAny(denyPrefixList, uri);
    }
    // No blacklist/allowList configured, all URLs are allowed
    return true;
  }

  @Override
  public String toString()
  {
    return "InputSourceSecurityConfig{" +
           "allowPrefexList=" + allowPrefexList +
           ", denyPrefixList=" + denyPrefixList +
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
    InputSourceSecurityConfig config = (InputSourceSecurityConfig) o;
    return Objects.equals(allowPrefexList, config.allowPrefexList) &&
           Objects.equals(denyPrefixList, config.denyPrefixList);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(allowPrefexList, denyPrefixList);
  }
}

