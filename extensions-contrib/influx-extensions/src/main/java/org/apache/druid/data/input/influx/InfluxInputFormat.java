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

package org.apache.druid.data.input.influx;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.utils.CompressionUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class InfluxInputFormat implements InputFormat
{
  public static final String TYPE_KEY = "influx";

  @Nullable
  private final List<String> whitelistMeasurements;

  @JsonCreator
  public InfluxInputFormat(
      @JsonProperty("whitelistMeasurements") @Nullable List<String> whitelistMeasurements
  )
  {
    this.whitelistMeasurements = whitelistMeasurements;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<String> getWhitelistMeasurements()
  {
    return whitelistMeasurements;
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory)
  {
    Set<String> whitelist = null;
    if (whitelistMeasurements != null && !whitelistMeasurements.isEmpty()) {
      whitelist = new HashSet<>(whitelistMeasurements);
    }
    return new InfluxLineProtocolReader(inputRowSchema, source, whitelist);
  }

  @Override
  public long getWeightedSize(String path, long size)
  {
    return size * CompressionUtils.estimatedCompressionFactor(CompressionUtils.Format.fromFileName(path));
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
    InfluxInputFormat that = (InfluxInputFormat) o;
    return Objects.equals(whitelistMeasurements, that.whitelistMeasurements);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(whitelistMeasurements);
  }
}
