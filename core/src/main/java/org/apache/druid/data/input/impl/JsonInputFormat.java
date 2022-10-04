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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

public class JsonInputFormat extends NestedInputFormat
{
  private final Map<String, Boolean> featureSpec;
  private final ObjectMapper objectMapper;
  private final boolean keepNullColumns;

  /**
   *
   * This parameter indicates whether or not the given InputEntity should be split by lines before parsing it.
   * If it is set to true, the InputEntity must be split by lines first.
   * If it is set to false, unlike what you could imagine, it means that the InputEntity doesn't have to be split by lines first, but it can still contain multiple lines.
   * A created InputEntityReader from this format will determine by itself if line splitting is necessary.
   *
   * This parameter should always be true for batch ingestion and false for streaming ingestion.
   * For more information, see: https://github.com/apache/druid/pull/10383.
   *
   */
  private final boolean lineSplittable;

  /**
   * If true, overrides the behavior controlled by lineSplittable and the parsing will assume that the input
   * is newline delimited. This is to allow streaming ingestion to use the newline delimited parser when the
   * input is known to follow that format, since this allows for more flexible handling of parse errors (i.e.,
   * an invalid JSON event on one line will not prevent other events on different lines from being ingested).
   */
  private final boolean assumeNewlineDelimited;

  /**
   * If true, use the JsonNodeReader when parsing non-newline delimited JSON. This parser splits the input string
   * into JsonNode objects, parsing each JsonNode into a separate InputRow, instead of parsing the input string
   * into several InputRow at the same time. This allows for more flexible handling of parse errors, where timestamp
   * parsing errors do not prevent other events in the same string from being ingested, and allows valid events prior to
   * encountering invalid JSON syntax to be ingested as well.
   */
  private final boolean useJsonNodeReader;

  @JsonCreator
  public JsonInputFormat(
      @JsonProperty("flattenSpec") @Nullable JSONPathSpec flattenSpec,
      @JsonProperty("featureSpec") @Nullable Map<String, Boolean> featureSpec,
      @JsonProperty("keepNullColumns") @Nullable Boolean keepNullColumns,
      @JsonProperty("assumeNewlineDelimited") @Nullable Boolean assumeNewlineDelimited,
      @JsonProperty("useJsonNodeReader") @Nullable Boolean useJsonNodeReader
  )
  {
    this(flattenSpec, featureSpec, keepNullColumns, true, assumeNewlineDelimited, useJsonNodeReader);
  }

  public JsonInputFormat(
      @Nullable JSONPathSpec flattenSpec,
      Map<String, Boolean> featureSpec,
      Boolean keepNullColumns,
      boolean lineSplittable,
      Boolean assumeNewlineDelimited,
      Boolean useJsonNodeReader
  )
  {
    super(flattenSpec);
    this.featureSpec = featureSpec == null ? Collections.emptyMap() : featureSpec;
    this.objectMapper = new ObjectMapper();
    if (keepNullColumns != null) {
      this.keepNullColumns = keepNullColumns;
    } else {
      this.keepNullColumns = flattenSpec != null && flattenSpec.isUseFieldDiscovery();
    }
    for (Entry<String, Boolean> entry : this.featureSpec.entrySet()) {
      Feature feature = Feature.valueOf(entry.getKey());
      objectMapper.configure(feature, entry.getValue());
    }
    this.lineSplittable = lineSplittable;
    this.assumeNewlineDelimited = assumeNewlineDelimited != null && assumeNewlineDelimited;
    this.useJsonNodeReader = useJsonNodeReader != null && useJsonNodeReader;
    if (this.assumeNewlineDelimited && this.useJsonNodeReader) {
      throw new IAE("useJsonNodeReader cannot be set to true when assumeNewlineDelimited is true.");
    }
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Map<String, Boolean> getFeatureSpec()
  {
    return featureSpec;
  }

  @JsonProperty // No @JsonInclude, since default is variable, so we can't assume false is default
  public boolean isKeepNullColumns()
  {
    return keepNullColumns;
  }

  @JsonProperty
  public boolean isAssumeNewlineDelimited()
  {
    return assumeNewlineDelimited;
  }

  @JsonProperty
  public boolean isUseJsonNodeReader()
  {
    return useJsonNodeReader;
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory)
  {
    if (this.lineSplittable || this.assumeNewlineDelimited) {
      return new JsonLineReader(inputRowSchema, source, getFlattenSpec(), objectMapper, keepNullColumns);
    } else if (this.useJsonNodeReader) {
      return new JsonNodeReader(inputRowSchema, source, getFlattenSpec(), objectMapper, keepNullColumns);
    } else {
      return new JsonReader(inputRowSchema, source, getFlattenSpec(), objectMapper, keepNullColumns);
    }
  }

  /**
   * Create a new JsonInputFormat object based on the given parameter
   *
   * sub-classes may need to override this method to create an object with correct sub-class type
   */
  public JsonInputFormat withLineSplittable(boolean lineSplittable)
  {
    return new JsonInputFormat(this.getFlattenSpec(),
                               this.getFeatureSpec(),
                               this.keepNullColumns,
                               lineSplittable,
                               assumeNewlineDelimited,
                               useJsonNodeReader
    );
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
    if (!super.equals(o)) {
      return false;
    }
    JsonInputFormat that = (JsonInputFormat) o;
    return keepNullColumns == that.keepNullColumns
           && lineSplittable == that.lineSplittable
           && assumeNewlineDelimited == that.assumeNewlineDelimited
           && useJsonNodeReader == that.useJsonNodeReader
           && Objects.equals(featureSpec, that.featureSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        super.hashCode(),
        featureSpec,
        keepNullColumns,
        lineSplittable,
        assumeNewlineDelimited,
        useJsonNodeReader
    );
  }
}
