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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.parsers.Parser;

@Deprecated
@ExtensionPoint
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "format")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "json", value = JSONParseSpec.class),
    @JsonSubTypes.Type(name = "csv", value = CSVParseSpec.class),
    @JsonSubTypes.Type(name = "tsv", value = DelimitedParseSpec.class),
    @JsonSubTypes.Type(name = "jsonLowercase", value = JSONLowercaseParseSpec.class),
    @JsonSubTypes.Type(name = "timeAndDims", value = TimeAndDimsParseSpec.class),
    @JsonSubTypes.Type(name = "regex", value = RegexParseSpec.class),
    @JsonSubTypes.Type(name = "javascript", value = JavaScriptParseSpec.class)

})
public abstract class ParseSpec
{
  private final TimestampSpec timestampSpec;
  private final DimensionsSpec dimensionsSpec;

  protected ParseSpec(TimestampSpec timestampSpec, DimensionsSpec dimensionsSpec)
  {
    this.timestampSpec = Preconditions.checkNotNull(timestampSpec, "parseSpec requires timestampSpec");
    this.dimensionsSpec = Preconditions.checkNotNull(dimensionsSpec, "parseSpec requires dimensionSpec");
  }

  @JsonProperty
  public TimestampSpec getTimestampSpec()
  {
    return timestampSpec;
  }

  @JsonProperty
  public DimensionsSpec getDimensionsSpec()
  {
    return dimensionsSpec;
  }

  public Parser<String, Object> makeParser()
  {
    return null;
  }

  @PublicApi
  public ParseSpec withTimestampSpec(TimestampSpec spec)
  {
    throw new UnsupportedOperationException();
  }

  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    throw new UnsupportedOperationException();
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

    ParseSpec parseSpec = (ParseSpec) o;

    if (timestampSpec != null ? !timestampSpec.equals(parseSpec.timestampSpec) : parseSpec.timestampSpec != null) {
      return false;
    }
    return !(dimensionsSpec != null
             ? !dimensionsSpec.equals(parseSpec.dimensionsSpec)
             : parseSpec.dimensionsSpec != null);

  }

  @Override
  public int hashCode()
  {
    int result = timestampSpec != null ? timestampSpec.hashCode() : 0;
    result = 31 * result + (dimensionsSpec != null ? dimensionsSpec.hashCode() : 0);
    return result;
  }
}
