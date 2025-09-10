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

package org.apache.druid.indexing.kafka.supervisor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.InDimFilter;

import javax.annotation.Nullable;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Kafka-specific implementation of header-based filtering.
 * Allows filtering Kafka records based on message headers before deserialization.
 */
public class KafkaHeaderBasedFilteringConfig
{
  private static final ImmutableSet<Class<? extends DimFilter>> SUPPORTED_FILTER_TYPES = ImmutableSet.of(
      InDimFilter.class
  );

  private final DimFilter filter;
  private final String encoding;
  private final int stringDecodingCacheSize;

  @JsonCreator
  public KafkaHeaderBasedFilteringConfig(
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("encoding") @Nullable String encoding,
      @JsonProperty("stringDecodingCacheSize") @Nullable Integer stringDecodingCacheSize
  )
  {
    this.filter = Preconditions.checkNotNull(filter, "filter cannot be null");
    this.encoding = encoding != null ? encoding : StandardCharsets.UTF_8.name();
    this.stringDecodingCacheSize = stringDecodingCacheSize != null ? stringDecodingCacheSize : 10_000;

    // Validate encoding
    try {
      Charset.forName(this.encoding);
    }
    catch (Exception e) {
      throw new IllegalArgumentException("Invalid encoding: " + this.encoding, e);
    }

    // Validate that only supported filter types are used
    validateSupportedFilter(this.filter);
  }

  /**
   * Validates that the filter is one of the supported types.
   * Only 'in' filters are supported for direct evaluation.
   */
  private void validateSupportedFilter(DimFilter dimFilter)
  {
    if (!SUPPORTED_FILTER_TYPES.contains(dimFilter.getClass())) {
      throw InvalidInput.exception(
          "Unsupported filter type [%s]. Only 'in' filters are supported for Kafka header filtering.",
          dimFilter.getClass().getSimpleName()
      );
    }
  }

  @JsonProperty
  public DimFilter getFilter()
  {
    return filter;
  }

  @JsonProperty
  public String getEncoding()
  {
    return encoding;
  }

  @JsonProperty
  public int getStringDecodingCacheSize()
  {
    return stringDecodingCacheSize;
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
    KafkaHeaderBasedFilteringConfig that = (KafkaHeaderBasedFilteringConfig) o;
    return stringDecodingCacheSize == that.stringDecodingCacheSize &&
           filter.equals(that.filter) &&
           encoding.equals(that.encoding);
  }

  @Override
  public int hashCode()
  {
    int result = filter.hashCode();
    result = 31 * result + encoding.hashCode();
    result = 31 * result + stringDecodingCacheSize;
    return result;
  }

  @Override
  public String toString()
  {
    return "KafkaHeaderBasedFilteringConfig{" +
           "filter=" + filter +
           ", encoding='" + encoding + '\'' +
           ", stringDecodingCacheSize=" + stringDecodingCacheSize +
           '}';
  }
}
