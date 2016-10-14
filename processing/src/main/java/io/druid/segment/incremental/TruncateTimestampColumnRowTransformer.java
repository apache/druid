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

package io.druid.segment.incremental;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import io.druid.data.input.InputRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.segment.Metadata;
import org.joda.time.DateTime;

import java.util.List;

public class TruncateTimestampColumnRowTransformer implements Function<InputRow, InputRow>
{
  private QueryGranularity granularity;
  private Metadata metadata;
  private String timestampColumn;
  private String timestampFormat;
  private Function<Long, String> timestampFormatter;

  public TruncateTimestampColumnRowTransformer(
      QueryGranularity granularity,
      Metadata metadata
  )
  {
    this.granularity = granularity;
    this.metadata = metadata;
    timestampColumn = metadata.getTimestampSpec().getTimestampColumn();
    timestampFormat = metadata.getTimestampSpec().getTimestampFormat();
    timestampFormatter = !"auto".equals(timestampFormat)
                         ? TimestampFormatter.createTimestampFormatter(timestampFormat) : null;
  }

  private boolean isMillis(String input)
  {
    for (int i = 0; i < input.length(); i++) {
      if (input.charAt(i) < '0' || input.charAt(i) > '9') {
        return false;
      }
    }
    return true;
  }

  private String getTimeStr(long timestamp)
  {
    return timestampFormatter.apply(granularity.truncate(timestamp));
  }

  @Override
  public InputRow apply(final InputRow row)
  {
    if ("auto".equals(timestampFormat)) {
      timestampFormat = isMillis(row.getDimension(timestampColumn).get(0)) ? "millis" : "iso";
      timestampFormatter = TimestampFormatter.createTimestampFormatter(timestampFormat);
      metadata.setTimestampSpec(
          new TimestampSpec(
              metadata.getTimestampSpec().getTimestampColumn(),
              timestampFormat,
              metadata.getTimestampSpec().getMissingValue()
          )
      );
    }

    return new InputRow()
    {
      @Override
      public List<String> getDimensions()
      {
        return row.getDimensions();
      }

      @Override
      public long getTimestampFromEpoch()
      {
        return row.getTimestampFromEpoch();
      }

      @Override
      public DateTime getTimestamp()
      {
        return row.getTimestamp();
      }

      @Override
      public List<String> getDimension(String dimension)
      {
        if (timestampColumn.equals(dimension)) {
          return ImmutableList.of(getTimeStr(row.getTimestampFromEpoch()));
        }
        return row.getDimension(dimension);
      }

      @Override
      public Object getRaw(String dimension)
      {
        if (timestampColumn.equals(dimension)) {
          return getTimeStr(row.getTimestampFromEpoch());
        }
        return row.getRaw(dimension);
      }

      @Override
      public long getLongMetric(String metric)
      {
        return row.getLongMetric(metric);
      }

      @Override
      public float getFloatMetric(String metric)
      {
        return row.getFloatMetric(metric);
      }

      @Override
      public String toString()
      {
        return row.toString();
      }

      @Override
      public int compareTo(Row o)
      {
        return getTimestamp().compareTo(o.getTimestamp());
      }
    };
  }
}
