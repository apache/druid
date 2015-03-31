/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.query.extraction;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.inject.name.Named;
import com.metamx.common.StringUtils;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.nio.ByteBuffer;

/**
 * Namespaced extraction is a special case of DimExtractionFn where the actual extractor is pulled from a map of known implementations.
 * In the event that an unknown namespace is passed, a simple reflective function is returned instead.
 */
public class NamespacedExtraction extends DimExtractionFn
{
  private static final byte CACHE_TYPE_ID = 0x05;
  private static final Function<String, String> NOOP_EXTRACTOR = new Function<String, String>()
  {
    @Nullable
    @Override
    public String apply(@Nullable String input)
    {
      // DimExtractionFn says do not return Empty strings
      return Strings.isNullOrEmpty(input) ? null : input;
    }
  };

  private final String namespace;
  private final String missingValue;
  private final Function<String, String> extractionFunction;

  @JsonCreator
  public NamespacedExtraction(
      @Nullable @JacksonInject @Named("dimExtractionNamespace")
      final Function<String, Function<String, String>> namespaces,
      @NotNull @JsonProperty(value = "namespace", required = true)
      final String namespace,
      @Nullable @JsonProperty(value = "missingValue", required = false)
      final String missingValue
  )
  {
    Preconditions.checkNotNull(namespace);
    this.namespace = namespace;
    if (namespaces == null) {
      this.extractionFunction = NOOP_EXTRACTOR;
    } else {
      final Function<String, String> fn = namespaces.apply(namespace);
      if (fn == null) {
        this.extractionFunction = NOOP_EXTRACTOR;
      } else {
        // If missing value is set, have a slightly different function.
        // This is intended to have the absolutely fastest code path possible and not have any extra logic in the function
        if (missingValue != null) {
          // Missing value is valid
          final String nullableMissingValue = Strings.isNullOrEmpty(missingValue) ? null : missingValue;
          this.extractionFunction = new Function<String, String>()
          {
            @Nullable
            @Override
            public String apply(@Nullable String dimValue)
            {
              final String retval = fn.apply(dimValue);
              return Strings.isNullOrEmpty(retval) ? nullableMissingValue : retval;
            }
          };
        } else {
          // Use dimValue if missing
          this.extractionFunction = new Function<String, String>()
          {
            @Nullable
            @Override
            public String apply(@Nullable String dimValue)
            {
              final String retval = fn.apply(dimValue);
              return Strings.isNullOrEmpty(retval) ? (Strings.isNullOrEmpty(dimValue) ? null : dimValue) : retval;
            }
          };
        }
      }
    }
    this.missingValue = missingValue;
  }

  @JsonProperty("namespace")
  public String getNamespace()
  {
    return this.namespace;
  }

  @JsonProperty("missingValue")
  public String getMissingValue() {return this.missingValue;}

  @Override
  public byte[] getCacheKey()
  {
    final byte[] nsBytes = StringUtils.toUtf8(namespace);
    return ByteBuffer.allocate(nsBytes.length + 1).put(CACHE_TYPE_ID).put(nsBytes).array();
  }

  @Override
  public String apply(String value)
  {
    return extractionFunction.apply(value);
  }

  @Override
  public boolean preservesOrdering()
  {
    return false;
  }
}
