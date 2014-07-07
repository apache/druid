/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexing.overlord.setup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.util.Charsets;
import org.apache.commons.codec.binary.Base64;

public class StringEC2UserData implements EC2UserData<StringEC2UserData>
{
  private final String data;
  private final String versionReplacementString;
  private final String version;

  @JsonCreator
  public StringEC2UserData(
      @JsonProperty("data") String data,
      @JsonProperty("versionReplacementString") String versionReplacementString,
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
      finalData = data.replace(versionReplacementString, version);
    } else {
      finalData = data;
    }
    return Base64.encodeBase64String(finalData.getBytes(Charsets.UTF_8));
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
