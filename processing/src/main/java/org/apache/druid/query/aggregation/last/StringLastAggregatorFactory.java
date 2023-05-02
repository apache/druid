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

package org.apache.druid.query.aggregation.last;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.query.aggregation.first.StringFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.StringFirstLastUtils;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.vector.BaseLongVectorValueSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonTypeName("stringLast")
public class StringLastAggregatorFactory extends AggregatorFactory
{
  public static final ColumnType TYPE = ColumnType.ofComplex("serializablePairLongString");

  @Nonnull
  public static Builder builder()
  {
    return new Builder();
  }

  @Nonnull
  public static Builder builder(String name, String fieldName)
  {
    return new Builder().setNames(name, fieldName);
  }

  private static final Aggregator NIL_AGGREGATOR = new StringLastAggregator(
      NilColumnValueSelector.instance(),
      NilColumnValueSelector.instance(),
      0,
      false
  )
  {
    @Override
    public void aggregate()
    {
      // no-op
    }
  };

  private static final BufferAggregator NIL_BUFFER_AGGREGATOR = new StringLastBufferAggregator(
      NilColumnValueSelector.instance(),
      NilColumnValueSelector.instance(),
      0,
      false
  )
  {
    @Override
    public void aggregate(ByteBuffer buf, int position)
    {
      // no-op
    }
  };

  private final String name;
  private final String fieldName;
  private final String timeColumn;
  private final String foldColumn;
  protected final int maxStringBytes;
  private final boolean finalize;

  @JsonCreator
  public StringLastAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") @Nullable final String fieldName,
      @JsonProperty("timeColumn") @Nullable final String timeColumn,
      @JsonProperty("foldColumn") @Nullable final String foldColumn,
      @JsonProperty("maxStringBytes") Integer maxStringBytes,
      @JsonProperty("finalize") @Nullable final Boolean finalize
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    if (maxStringBytes != null && maxStringBytes < 0) {
      throw new IAE("maxStringBytes must be greater than 0");
    }

    if (foldColumn == null) {
      Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName or a foldColumn");

      this.fieldName = fieldName;
      this.timeColumn = timeColumn == null ? ColumnHolder.TIME_COLUMN_NAME : timeColumn;
      this.foldColumn = null;
    } else {
      if (fieldName != null || timeColumn != null) {
        throw new IAE(
            "If foldColumn [%s] is set, neither fieldName [%s] nor timeColumn [%s] can be set.",
            foldColumn,
            fieldName,
            timeColumn
        );
      }
      this.fieldName = null;
      this.timeColumn = null;
      this.foldColumn = foldColumn;
    }

    if (maxStringBytes != null && maxStringBytes < 0) {
      throw new IAE("maxStringBytes must be greater than 0");
    }

    this.name = name;
    this.maxStringBytes = maxStringBytes == null
                          ? StringFirstAggregatorFactory.DEFAULT_MAX_STRING_SIZE
                          : maxStringBytes;
    this.finalize = finalize == null ? true : finalize;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    if (foldColumn != null) {
      final ColumnValueSelector<SerializablePairLongString> selector = metricFactory.makeColumnValueSelector(foldColumn);
      if (selector instanceof NilColumnValueSelector) {
        return NIL_AGGREGATOR;
      } else {
        return new StringLastFoldingAggregator(selector, maxStringBytes);
      }
    }

    final BaseObjectColumnValueSelector<?> valueSelector = metricFactory.makeColumnValueSelector(fieldName);
    if (valueSelector instanceof NilColumnValueSelector) {
      return NIL_AGGREGATOR;
    } else {
      return new StringLastAggregator(
          metricFactory.makeColumnValueSelector(timeColumn),
          valueSelector,
          maxStringBytes,
          StringFirstLastUtils.selectorNeedsFoldCheck(valueSelector, metricFactory.getColumnCapabilities(fieldName))
      );
    }
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    if (foldColumn != null) {
      final ColumnValueSelector<SerializablePairLongString> selector = metricFactory.makeColumnValueSelector(foldColumn);
      if (selector instanceof NilColumnValueSelector) {
        return NIL_BUFFER_AGGREGATOR;
      } else {
        return new StringLastFoldingBufferAggregator(selector, maxStringBytes);
      }
    }

    final BaseObjectColumnValueSelector<?> valueSelector = metricFactory.makeColumnValueSelector(fieldName);
    if (valueSelector instanceof NilColumnValueSelector) {
      return NIL_BUFFER_AGGREGATOR;
    } else {
      return new StringLastBufferAggregator(
          metricFactory.makeColumnValueSelector(timeColumn),
          valueSelector,
          maxStringBytes,
          StringFirstLastUtils.selectorNeedsFoldCheck(valueSelector, metricFactory.getColumnCapabilities(fieldName))
      );
    }
  }

  @Override
  public boolean canVectorize(ColumnInspector columnInspector)
  {
    return true;
  }

  @Override
  public VectorAggregator factorizeVector(VectorColumnSelectorFactory selectorFactory)
  {

    ColumnCapabilities capabilities = selectorFactory.getColumnCapabilities(fieldName);
    VectorObjectSelector vSelector = selectorFactory.makeObjectSelector(fieldName);
    BaseLongVectorValueSelector timeSelector = (BaseLongVectorValueSelector) selectorFactory.makeValueSelector(
        timeColumn);
    if (capabilities != null) {
      return new StringLastVectorAggregator(timeSelector, vSelector, maxStringBytes);
    } else {
      return new StringLastVectorAggregator(null, vSelector, maxStringBytes);
    }

  }

  @Override
  public Comparator getComparator()
  {
    return StringFirstAggregatorFactory.VALUE_COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return StringFirstAggregatorFactory.TIME_COMPARATOR.compare(lhs, rhs) > 0 ? lhs : rhs;
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return new StringLastAggregateCombiner();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new Builder(this).setFieldName(null).setTimeColumn(null).setFoldColumn(name).build();
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(new Builder(this).setName(fieldName).build());
  }

  @Override
  public Object deserialize(Object object)
  {
    Map map = (Map) object;
    return new SerializablePairLongString(((Number) map.get("lhs")).longValue(), ((String) map.get("rhs")));
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    if (finalize) {
      return object == null ? null : ((Pair) object).rhs;
    } else {
      return object;
    }
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getTimeColumn()
  {
    return timeColumn;
  }

  @JsonProperty("foldColumn")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getFoldColumn()
  {
    return foldColumn;
  }

  @JsonProperty
  public Integer getMaxStringBytes()
  {
    return maxStringBytes;
  }

  @JsonProperty("finalize")
  public boolean isFinalize()
  {
    return finalize;
  }

  @Override
  public List<String> requiredFields()
  {
    return ImmutableList.of(timeColumn, fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.STRING_LAST_CACHE_TYPE_ID)
        .appendString(fieldName)
        .appendString(timeColumn)
        .appendInt(maxStringBytes)
        .build();
  }

  /**
   * actual type is {@link SerializablePairLongString}
   */
  @Override
  public ColumnType getIntermediateType()
  {
    return TYPE;
  }

  @Override
  public ColumnType getResultType()
  {
    return finalize ? ColumnType.STRING : TYPE;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Long.BYTES + Integer.BYTES + maxStringBytes;
  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new Builder(this).setName(newName).build();
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
    StringLastAggregatorFactory that = (StringLastAggregatorFactory) o;
    return maxStringBytes == that.maxStringBytes &&
           Objects.equals(fieldName, that.fieldName) &&
           Objects.equals(name, that.name);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(fieldName, name, maxStringBytes);
  }

  @Override
  public String toString()
  {
    return "StringLastAggregatorFactory{" +
           "fieldName='" + fieldName + '\'' +
           ", name='" + name + '\'' +
           ", maxStringBytes=" + maxStringBytes +
           '}';
  }

  public static class Builder
  {
    private String name;
    private String fieldName;
    private String timeColumn;
    private String foldColumn;
    private Integer maxStringBytes = null;
    private boolean finalize = true;

    public Builder()
    {

    }

    public Builder(StringLastAggregatorFactory factory)
    {
      this.name = factory.getName();
      this.fieldName = factory.getFieldName();
      this.timeColumn = factory.getTimeColumn();
      this.foldColumn = factory.getFoldColumn();
      this.maxStringBytes = factory.getMaxStringBytes();
      this.finalize = factory.finalize;
    }

    public Builder copy()
    {
      final Builder retVal = new Builder();
      retVal.name = name;
      retVal.fieldName = fieldName;
      retVal.timeColumn = timeColumn;
      retVal.foldColumn = foldColumn;
      retVal.maxStringBytes = maxStringBytes;
      retVal.finalize = finalize;
      return retVal;
    }

    public Builder setNames(String name, String fieldName)
    {
      return setName(name).setFieldName(fieldName);
    }

    public Builder setName(String name)
    {
      this.name = name;
      return this;
    }

    public Builder setFieldName(String fieldName)
    {
      this.fieldName = fieldName;
      return this;
    }

    public Builder setTimeColumn(String timeColumn)
    {
      this.timeColumn = timeColumn;
      return this;
    }

    public Builder setFoldColumn(String foldColumn)
    {
      this.foldColumn = foldColumn;
      return this;
    }

    public Builder setMaxStringBytes(Integer maxStringBytes)
    {
      this.maxStringBytes = maxStringBytes;
      return this;
    }

    public Builder setFinalize(boolean finalize)
    {
      this.finalize = finalize;
      return this;
    }

    public StringLastAggregatorFactory build()
    {
      return new StringLastAggregatorFactory(name, fieldName, timeColumn, foldColumn, maxStringBytes, finalize);
    }
  }
}
