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

package org.apache.druid.indexing.overlord.setup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class WorkerCategorySpec
{
  // key: taskType, value: categoryConfig
  private final Map<String, CategoryConfig> categoryMap;
  private final boolean strong;

  @JsonCreator
  public WorkerCategorySpec(
      @JsonProperty("categoryMap") Map<String, CategoryConfig> categoryMap,
      @JsonProperty("strong") boolean strong
  )
  {
    this.categoryMap = categoryMap == null ? Collections.EMPTY_MAP : categoryMap;
    this.strong = strong;
  }

  @JsonProperty
  public Map<String, CategoryConfig> getCategoryMap()
  {
    return categoryMap;
  }

  @JsonProperty
  public boolean isStrong()
  {
    return strong;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final WorkerCategorySpec that = (WorkerCategorySpec) o;
    return strong == that.strong &&
           Objects.equals(categoryMap, that.categoryMap);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(categoryMap, strong);
  }

  @Override
  public String toString()
  {
    return "WorkerCategorySpec{" +
           "categoryMap=" + categoryMap +
           ", strong=" + strong +
           '}';
  }

  public static class CategoryConfig
  {
    private final String defaultCategory;
    // key: datasource, value: category
    private final Map<String, String> categoryAffinity;

    @JsonCreator
    public CategoryConfig(
        @JsonProperty("defaultCategory") String defaultCategory,
        @JsonProperty("categoryAffinity") Map<String, String> categoryAffinity
    )
    {
      this.defaultCategory = defaultCategory;
      this.categoryAffinity = categoryAffinity == null ? Collections.EMPTY_MAP : categoryAffinity;
    }

    @JsonProperty
    public String getDefaultCategory()
    {
      return defaultCategory;
    }

    @JsonProperty
    public Map<String, String> getCategoryAffinity()
    {
      return categoryAffinity;
    }

    @Override
    public boolean equals(final Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final CategoryConfig that = (CategoryConfig) o;
      return Objects.equals(defaultCategory, that.defaultCategory) &&
             Objects.equals(categoryAffinity, that.categoryAffinity);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(defaultCategory, categoryAffinity);
    }

    @Override
    public String toString()
    {
      return "CategoryConfig{" +
             "defaultCategory=" + defaultCategory +
             ", categoryAffinity=" + categoryAffinity +
             '}';
    }
  }
}
