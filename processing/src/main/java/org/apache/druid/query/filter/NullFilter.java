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

package org.apache.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcherColumnProcessorFactory;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.index.AllTrueBitmapColumnIndex;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.index.semantic.DruidPredicateIndexes;
import org.apache.druid.segment.index.semantic.NullValueIndex;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class NullFilter extends AbstractOptimizableDimFilter implements Filter
{
  public static NullFilter forColumn(String column)
  {
    return new NullFilter(column, null);
  }

  private final String column;
  @Nullable
  private final FilterTuning filterTuning;

  @JsonCreator
  public NullFilter(
      @JsonProperty("column") String column,
      @JsonProperty("filterTuning") @Nullable FilterTuning filterTuning
  )
  {
    if (column == null) {
      throw InvalidInput.exception("Invalid null filter, column cannot be null");
    }
    this.column = column;
    this.filterTuning = filterTuning;
  }

  @JsonProperty
  public String getColumn()
  {
    return column;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public FilterTuning getFilterTuning()
  {
    return filterTuning;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(DimFilterUtils.NULL_CACHE_ID)
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendString(column)
        .build();
  }

  @Override
  public Filter toFilter()
  {
    return this;
  }

  @Nullable
  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    if (!Objects.equals(getColumn(), dimension)) {
      return null;
    }

    RangeSet<String> retSet = TreeRangeSet.create();
    // Nulls are less than empty String in segments
    retSet.add(Range.lessThan(""));
    return retSet;
  }

  @Nullable
  @Override
  public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector selector)
  {
    if (!Filters.checkFilterTuningUseIndex(column, selector, filterTuning)) {
      return null;
    }
    final ColumnIndexSupplier indexSupplier = selector.getIndexSupplier(column);
    if (indexSupplier == null) {
      return new AllTrueBitmapColumnIndex(selector);
    }
    final NullValueIndex nullValueIndex = indexSupplier.as(NullValueIndex.class);
    if (nullValueIndex != null) {
      return nullValueIndex.get();
    }
    final DruidPredicateIndexes predicateIndexes = indexSupplier.as(DruidPredicateIndexes.class);
    if (predicateIndexes != null) {
      return predicateIndexes.forPredicate(NullPredicateFactory.INSTANCE);
    }
    return null;
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    return Filters.makeValueMatcher(factory, column, NullPredicateFactory.INSTANCE);
  }

  @Override
  public VectorValueMatcher makeVectorMatcher(VectorColumnSelectorFactory factory)
  {
    return ColumnProcessors.makeVectorProcessor(
        column,
        VectorValueMatcherColumnProcessorFactory.instance(),
        factory
    ).makeMatcher(NullPredicateFactory.INSTANCE);
  }

  @Override
  public boolean canVectorizeMatcher(ColumnInspector inspector)
  {
    return true;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return ImmutableSet.of(column);
  }

  @Override
  public boolean supportsRequiredColumnRewrite()
  {
    return true;
  }

  @Override
  public Filter rewriteRequiredColumns(Map<String, String> columnRewrites)
  {
    String rewriteDimensionTo = columnRewrites.get(column);

    if (rewriteDimensionTo == null) {
      throw new IAE(
          "Received a non-applicable rewrite: %s, filter's dimension: %s",
          columnRewrites,
          columnRewrites
      );
    }
    return new NullFilter(rewriteDimensionTo, filterTuning);
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
    NullFilter that = (NullFilter) o;
    return Objects.equals(column, that.column) &&
           Objects.equals(filterTuning, that.filterTuning);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(column, filterTuning);
  }

  @Override
  public String toString()
  {
    return new DimFilterToStringBuilder().appendDimension(column, null)
                                         .append(" IS NULL")
                                         .appendFilterTuning(filterTuning)
                                         .build();
  }

  public static class NullPredicateFactory implements DruidPredicateFactory
  {
    public static final NullPredicateFactory INSTANCE = new NullPredicateFactory();

    private NullPredicateFactory()
    {
      // no instantiation
    }

    @Override
    public DruidObjectPredicate<String> makeStringPredicate()
    {
      return DruidObjectPredicate.isNull();
    }

    @Override
    public DruidLongPredicate makeLongPredicate()
    {
      return DruidLongPredicate.MATCH_NULL_ONLY;
    }

    @Override
    public DruidFloatPredicate makeFloatPredicate()
    {
      return DruidFloatPredicate.MATCH_NULL_ONLY;
    }

    @Override
    public DruidDoublePredicate makeDoublePredicate()
    {
      return DruidDoublePredicate.MATCH_NULL_ONLY;
    }

    @Override
    public DruidObjectPredicate<Object[]> makeArrayPredicate(@Nullable TypeSignature<ValueType> arrayType)
    {
      return DruidObjectPredicate.isNull();
    }

    @Override
    public DruidObjectPredicate<Object> makeObjectPredicate()
    {
      return DruidObjectPredicate.isNull();
    }

    @Override
    public int hashCode()
    {
      return super.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      return true;
    }

    @Override
    public String toString()
    {
      return "NullPredicateFactory{}";
    }
  }
}
