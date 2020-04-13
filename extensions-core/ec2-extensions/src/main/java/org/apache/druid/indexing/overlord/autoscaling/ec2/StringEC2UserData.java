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

package org.apache.druid.indexing.overlord.autoscaling.ec2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.util.Objects;

public class StringEC2UserData implements EC2UserData<StringEC2UserData>
{
  private final String data;
  @Nullable
  private final String versionReplacementString;
  private final String version;

  @JsonCreator
  public StringEC2UserData(
      @JsonProperty("data") String data,
      @JsonProperty("versionReplacementString") @Nullable String versionReplacementString,
      @JsonProperty("version") String version
  )
  {
    this.data = data;
    this.versionReplacementString = versionReplacementString;
    this.version = version;
  }

  @JsonProperty
  public String getData()
  {
    return data;
  }

  @Nullable
  @JsonProperty
  public String getVersionReplacementString()
  {
    return versionReplacementString;
  }

  @JsonProperty
  public String getVersion()
  {
    return version;
  }

  @Override
  public StringEC2UserData withVersion(final String _version)
  {
    return new StringEC2UserData(data, versionReplacementString, _version);
  }

  @Override
  public String getUserDataBase64()
  {
    final String finalData;
    if (versionReplacementString != null && version != null) {
      finalData = StringUtils.replace(data, versionReplacementString, version);
    } else {
      finalData = data;
    }
    return StringUtils.encodeBase64String(StringUtils.toUtf8(finalData));
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

    StringEC2UserData that = (StringEC2UserData) o;

    if (data != null ? !data.equals(that.data) : that.data != null) {
      return false;
    }
    if (!Objects.equals(version, that.version)) {
      return false;
    }
    return Objects.equals(versionReplacementString, that.versionReplacementString);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(data, versionReplacementString, version);
  }

  @Override
  public String toString()
  {
    return "StringEC2UserData{" +
           "data='" + data + '\'' +
           ", versionReplacementString='" + versionReplacementString + '\'' +
           ", version='" + version + '\'' +
           '}';
  }
}
