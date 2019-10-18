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

package org.apache.druid.query.aggregation.collectset;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.ObjectAggregateCombiner;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CollectSetAggregatorFactory extends AggregatorFactory
{
  static final Comparator<Set<Object>> COMPARATOR =
      Comparator.nullsFirst(Comparator.comparingDouble(Set::size));

  private final String name;
  private final String fieldName;

  @JsonCreator
  public CollectSetAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName
  )
  {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(fieldName);
    this.name = name;
    this.fieldName = fieldName;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory columnSelectorFactory)
  {
    final ColumnValueSelector<Object> selector =
        columnSelectorFactory.makeColumnValueSelector(getFieldName());
    return new CollectSetAggregator(selector);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnSelectorFactory)
  {
    final ColumnValueSelector<Object> selector =
        columnSelectorFactory.makeColumnValueSelector(getFieldName());
    return new CollectSetBufferAggregator(selector);
  }

  @Override
  public Comparator getComparator()
  {
    return COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    Set<Object> set = new HashSet<>();
    if (lhs == null && rhs == null) {
      return set;
    } else if (rhs == null) {
      set.addAll((Collection) lhs);
    } else if (lhs == null) {
      set.addAll((Collection) rhs);
    } else {
      set.addAll((Collection) lhs);
      set.addAll((Collection) rhs);
    }

    return set;
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return new ObjectAggregateCombiner<Set>()
    {
      private final Set<Object> unionSet = new HashSet<>();

      @Override
      public void reset(final ColumnValueSelector selector)
      {
        unionSet.clear();
        fold(selector);
      }

      @Override
      public void fold(final ColumnValueSelector selector)
      {
        if (Set.class.equals(selector.classOfObject())) {
          unionSet.addAll((Set<Object>) selector.getObject());
        } else {
          throw new IAE("Aggregate combiner can only fold a set for field[%s].", fieldName);
        }
      }

      @Nullable
      @Override
      public Set<Object> getObject()
      {
        return new HashSet<>(unionSet);
      }

      @Override
      public Class<Set> classOfObject()
      {
        return Set.class;
      }
    };
  }


  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new CollectSetAggregatorFactory(name, name);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(
        new CollectSetAggregatorFactory(fieldName, fieldName)
    );
  }

  @Override
  public Object deserialize(Object object)
  {
    return object;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return object;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);
    return ByteBuffer.allocate(1 + fieldNameBytes.length)
                     .put(AggregatorUtil.COLLECT_SET_CACHE_TYPE_ID)
                     .put(fieldNameBytes)
                     .array();
  }

  @Override
  public String getTypeName()
  {
    return CollectSetDruidModule.TYPE_NAME;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Long.BYTES;
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

    CollectSetAggregatorFactory that = (CollectSetAggregatorFactory) o;

    if (!fieldName.equals(that.fieldName)) {
      return false;
    }
    if (!name.equals(that.name)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + fieldName.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "CollectSetAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           '}';
  }
}
