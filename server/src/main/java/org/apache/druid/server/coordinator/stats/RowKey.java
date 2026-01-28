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

package org.apache.druid.server.coordinator.stats;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a row key against which stats are reported.
 */
public class RowKey
{
  private static final RowKey EMPTY = new RowKey(Collections.emptyMap());

  private final Map<Dimension, String> values;
  private final int hashCode;

  private RowKey(Map<Dimension, String> values)
  {
    this.values = values;
    this.hashCode = Objects.hash(values);
  }

  public static Builder with(Dimension dimension, String value)
  {
    Builder builder = new Builder();
    builder.with(dimension, value);
    return builder;
  }

  public static RowKey of(Dimension dimension, String value)
  {
    return with(dimension, value).build();
  }

  public static RowKey empty()
  {
    return EMPTY;
  }

  public Map<Dimension, String> getValues()
  {
    return values;
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
    RowKey that = (RowKey) o;
    return Objects.equals(values, that.values);
  }

  @Override
  public int hashCode()
  {
    return hashCode;
  }

  public static class Builder
  {
    private final Map<Dimension, String> values = new EnumMap<>(Dimension.class);

    public Builder with(Dimension dimension, String value)
    {
      values.put(dimension, value);
      return this;
    }

    public RowKey and(Dimension dimension, String value)
    {
      values.put(dimension, value);
      return new RowKey(values);
    }

    public RowKey build()
    {
      return new RowKey(values);
    }
  }

  @Override
  public String toString()
  {
    return values == null ? "{}" : values.toString();
  }
}
