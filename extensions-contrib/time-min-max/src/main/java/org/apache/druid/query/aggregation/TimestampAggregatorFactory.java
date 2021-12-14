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

package org.apache.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnType;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public abstract class TimestampAggregatorFactory extends AggregatorFactory
{
  public static final ColumnType FINALIZED_TYPE = ColumnType.ofComplex("dateTime");
  final String name;
  @Nullable
  final String fieldName;
  @Nullable
  final String timeFormat;
  private final Comparator<Long> comparator;
  private final Long initValue;

  private TimestampSpec timestampSpec;

  TimestampAggregatorFactory(
      String name,
      @Nullable String fieldName,
      @Nullable String timeFormat,
      Comparator<Long> comparator,
      Long initValue
  )
  {
    this.name = name;
    this.fieldName = fieldName;
    this.timeFormat = timeFormat;
    this.comparator = comparator;
    this.initValue = initValue;

    this.timestampSpec = new TimestampSpec(fieldName, timeFormat, null);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new TimestampAggregator(
        metricFactory.makeColumnValueSelector(timestampSpec.getTimestampColumn()),
        timestampSpec,
        comparator,
        initValue
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new TimestampBufferAggregator(
        metricFactory.makeColumnValueSelector(timestampSpec.getTimestampColumn()),
        timestampSpec,
        comparator,
        initValue
    );
  }

  @Override
  public Comparator getComparator()
  {
    return TimestampAggregator.COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return TimestampAggregator.combineValues(comparator, lhs, rhs);
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    // TimestampAggregatorFactory.combine() delegates to TimestampAggregator.combineValues() and it doesn't check
    // for nulls, so this AggregateCombiner neither.
    return new LongAggregateCombiner()
    {
      private long result;

      @Override
      public void reset(ColumnValueSelector selector)
      {
        result = getTimestamp(selector);
      }

      private long getTimestamp(ColumnValueSelector selector)
      {
        if (Long.class.equals(selector.classOfObject())) {
          return selector.getLong();
        } else {
          Object input = selector.getObject();
          return convertLong(timestampSpec, input);
        }
      }

      @Override
      public void fold(ColumnValueSelector selector)
      {
        long other = getTimestamp(selector);
        if (comparator.compare(result, other) <= 0) {
          result = other;
        }
      }

      @Override
      public long getLong()
      {
        return result;
      }
    };
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && this.getClass() == other.getClass()) {
      return getCombiningFactory();
    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  @Override
  public Object deserialize(Object object)
  {
    return object;
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    return object == null ? null : DateTimes.utc((long) object);
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Nullable
  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Nullable
  @JsonProperty
  public String getTimeFormat()
  {
    return timeFormat;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(timestampSpec.getTimestampColumn());
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.TIMESTAMP_CACHE_TYPE_ID).appendString(timestampSpec.getTimestampColumn())
                                                                      .appendString(timestampSpec.getTimestampFormat())
                                                                      .build();
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return ColumnType.LONG;
  }

  /**
   * actual type is {@link DateTime}
   */
  @Override
  public ColumnType getResultType()
  {
    return FINALIZED_TYPE;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Long.BYTES;
  }


  @Nullable
  static Long convertLong(TimestampSpec timestampSpec, Object input)
  {
    if (input instanceof Number) {
      return ((Number) input).longValue();
    } else if (input instanceof DateTime) {
      return ((DateTime) input).getMillis();
    } else if (input instanceof Timestamp) {
      return ((Timestamp) input).getTime();
    } else if (input instanceof String) {
      return timestampSpec.parseDateTime(input).getMillis();
    }

    return null;
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
    TimestampAggregatorFactory that = (TimestampAggregatorFactory) o;
    return name.equals(that.name) && Objects.equals(fieldName, that.fieldName) && Objects.equals(
        timeFormat,
        that.timeFormat
    ) && comparator.equals(that.comparator) && initValue.equals(that.initValue);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName, timeFormat, comparator, initValue);
  }
}
