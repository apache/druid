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

package org.apache.druid.mapStringString;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.ParseSpec;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MapStringStringSelectorInputRowParser<T> implements InputRowParser<T>
{
  private final InputRowParser<T> delegate;
  private final String mapColumnName;
  private final Set<String> topLevelColumnKeys;

  public MapStringStringSelectorInputRowParser(
      @JsonProperty("delegate") final InputRowParser<T> delegate,
      @JsonProperty("mapColumnName") final String mapColumnName,
      @JsonProperty("topLevelColumnKeys") final Set<String> topLevelColumnKeys
  )
  {
    this.delegate = delegate;
    this.mapColumnName = mapColumnName;
    this.topLevelColumnKeys = topLevelColumnKeys;
  }

  @JsonProperty
  public InputRowParser<T> getDelegate()
  {
    return delegate;
  }

  @JsonProperty
  public String getMapColumnName()
  {
    return mapColumnName;
  }

  @JsonProperty
  public Set<String> getTopLevelColumnKeys()
  {
    return topLevelColumnKeys;
  }

  @Override
  public List<InputRow> parseBatch(final T row)
  {
    return delegate.parseBatch(row).stream().map(TransformedInputRow::new).collect(Collectors.toList());
  }

  @Override
  public ParseSpec getParseSpec()
  {
    return delegate.getParseSpec();
  }

  @Override
  @SuppressWarnings("unchecked")
  public InputRowParser<T> withParseSpec(final ParseSpec parseSpec)
  {
    return new MapStringStringSelectorInputRowParser<>(delegate.withParseSpec(parseSpec), mapColumnName, topLevelColumnKeys);
  }

  private class TransformedInputRow implements InputRow
  {
    private final InputRow delegate;
    private final List<String> dimensions;

    public TransformedInputRow(InputRow delegate)
    {
      this.delegate = delegate;

      if (delegate.getDimensions() == null || delegate.getDimensions().isEmpty()) {
        dimensions = Collections.emptyList();
      } else {
        dimensions = new ArrayList<>(Math.min(topLevelColumnKeys.size(), delegate.getDimensions().size()) + 1);

        for (String dim : delegate.getDimensions()) {
          if (topLevelColumnKeys.contains(dim)) {
            dimensions.add(dim);
          }
        }

        dimensions.add(mapColumnName);
      }
    }

    @Override
    public List<String> getDimensions()
    {
      return dimensions;
    }

    @Override
    public long getTimestampFromEpoch()
    {
      return delegate.getTimestampFromEpoch();
    }

    @Override
    public DateTime getTimestamp()
    {
      return delegate.getTimestamp();
    }

    @Override
    public List<String> getDimension(String dimension)
    {
      if (topLevelColumnKeys.contains(dimension)) {
        return delegate.getDimension(dimension);
      } else {
        return null;
      }
    }

    @Nullable
    @Override
    public Object getRaw(String dimension)
    {
      if (topLevelColumnKeys.contains(dimension)) {
        return delegate.getRaw(dimension);
      } else if (mapColumnName.equals(dimension)) {
        Map<String, String> data = new HashMap<>();
        for (String dim : delegate.getDimensions()) {
          if (!topLevelColumnKeys.contains(dim)) {
            Object value = delegate.getRaw(dim);
            data.put(dim, (String) value);
          }
        }
        return data;
      } else {
        return null;
      }
    }

    @Nullable
    @Override
    public Number getMetric(String metric)
    {
      return delegate.getMetric(metric);
    }

    @Override
    public int compareTo(Row o)
    {
      return delegate.compareTo(o);
    }
  }
}
