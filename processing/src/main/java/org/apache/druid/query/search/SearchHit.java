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

package org.apache.druid.query.search;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.common.config.NullHandling;

/**
 */
public class SearchHit implements Comparable<SearchHit>
{
  private final String dimension;
  private final String value;
  private final Integer count;

  @JsonCreator
  public SearchHit(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("value") String value,
      @JsonProperty("count") Integer count
  )
  {
    this.dimension = Preconditions.checkNotNull(dimension);
    this.value = NullHandling.nullToEmptyIfNeeded(value);
    this.count = count;
  }

  public SearchHit(String dimension, String value)
  {
    this(dimension, value, null);
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public String getValue()
  {
    return value;
  }

  @JsonProperty
  public Integer getCount()
  {
    return count;
  }

  @Override
  public int compareTo(SearchHit o)
  {
    int retVal = dimension.compareTo(o.dimension);
    if (retVal == 0) {
      retVal = value.compareTo(o.value);
    }
    return retVal;
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

    SearchHit searchHit = (SearchHit) o;

    if (dimension != null ? !dimension.equals(searchHit.dimension) : searchHit.dimension != null) {
      return false;
    }
    if (value != null ? !value.equals(searchHit.value) : searchHit.value != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = dimension != null ? dimension.hashCode() : 0;
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "Hit{" +
           "dimension='" + dimension + '\'' +
           ", value='" + value + '\'' +
           (count != null ? ", count='" + count + '\'' : "") +
           '}';
  }
}
