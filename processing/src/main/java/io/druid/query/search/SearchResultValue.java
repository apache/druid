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

package io.druid.query.search;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.druid.query.search.search.SearchHit;

import java.util.Iterator;
import java.util.List;

/**
 */
public class SearchResultValue implements Iterable<SearchHit>
{
  private final List<SearchHit> value;

  @JsonCreator
  public SearchResultValue(
      List<SearchHit> value
  )
  {
    this.value = value;
  }

  @JsonValue
  public List<SearchHit> getValue()
  {
    return value;
  }

  @Override
  public Iterator<SearchHit> iterator()
  {
    return value.iterator();
  }

  @Override
  public String toString()
  {
    return "SearchResultValue{" +
           "value=" + value +
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

    SearchResultValue that = (SearchResultValue) o;

    if (value != null ? !value.equals(that.value) : that.value != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return value != null ? value.hashCode() : 0;
  }
}
