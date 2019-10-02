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

package org.apache.druid.query.aggregation.mean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.collect.Utils;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 */
public class DoubleMeanAggregatorFactory extends AggregatorFactory
{
  private final String name;
  private final String fieldName;

  @JsonCreator
  public DoubleMeanAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName
  )
  {
    this.name = Preconditions.checkNotNull(name, "null name");
    this.fieldName = Preconditions.checkNotNull(fieldName, "null fieldName");
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public String getTypeName()
  {
    return "doubleMean";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return DoubleMeanHolder.MAX_INTERMEDIATE_SIZE;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new DoubleMeanAggregator(metricFactory.makeColumnValueSelector(fieldName));
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new DoubleMeanBufferAggregator(metricFactory.makeColumnValueSelector(fieldName));
  }

  @Override
  public VectorAggregator factorizeVector(final VectorColumnSelectorFactory selectorFactory)
  {
    return new DoubleMeanVectorAggregator(selectorFactory.makeValueSelector(fieldName));
  }

  @Override
  public boolean canVectorize()
  {
    return true;
  }

  @Override
  public Comparator getComparator()
  {
    return DoubleMeanHolder.COMPARATOR;
  }

  @Nullable
  @Override
  public Object combine(@Nullable Object lhs, @Nullable Object rhs)
  {
    if (lhs instanceof DoubleMeanHolder && rhs instanceof DoubleMeanHolder) {
      return ((DoubleMeanHolder) lhs).update((DoubleMeanHolder) rhs);
    } else {
      throw new IAE(
          "lhs[%s] or rhs[%s] not of type [%s]",
          Utils.safeObjectClassGetName(lhs),
          Utils.safeObjectClassGetName(rhs),
          DoubleMeanHolder.class.getName()
      );
    }
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DoubleMeanAggregatorFactory(name, name);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(new DoubleMeanAggregatorFactory(fieldName, fieldName));
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object instanceof String) {
      return DoubleMeanHolder.fromBytes(StringUtils.decodeBase64(StringUtils.toUtf8((String) object)));
    } else if (object instanceof DoubleMeanHolder) {
      return object;
    } else {
      throw new IAE("Unknown object type [%s]", Utils.safeObjectClassGetName(object));
    }
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    if (object instanceof DoubleMeanHolder) {
      return ((DoubleMeanHolder) object).mean();
    } else if (object == null) {
      return null;
    } else {
      throw new IAE("Unknown object type [%s]", object.getClass().getName());
    }
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.MEAN_CACHE_TYPE_ID)
        .appendString(name)
        .appendString(fieldName)
        .build();
  }
}
