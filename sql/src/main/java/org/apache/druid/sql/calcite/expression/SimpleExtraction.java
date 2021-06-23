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

package org.apache.druid.sql.calcite.expression;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.dimension.ListFilteredDimensionSpec;
import org.apache.druid.query.dimension.PrefixFilteredDimensionSpec;
import org.apache.druid.query.dimension.RegexFilteredDimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * Represents a "simple" extraction of a value from a Druid row, which is defined as a column plus an optional
 * extractionFn. This is useful since identifying simple extractions and treating them specially can allow Druid to
 * perform additional optimizations.
 */
public class SimpleExtraction
{
  private final String column;
  @Nullable
  private final ExtractionFn extractionFn;
  @Nullable
  private final FilteredDimensionSpacesOperands fdsOperands;

  public static class FilteredDimensionSpacesOperands
  {
    private final FilteredDimensionSpacesType filteredDimensionSpacesType;
    private final List<String> operands;

    public FilteredDimensionSpacesOperands(String funcName, List<String> operands)
    {
      this.filteredDimensionSpacesType = FilteredDimensionSpacesType.fromString(funcName);
      if (filteredDimensionSpacesType == null) {
        throw new IAE("invalid filtered dimension function name: [%s]", funcName);
      }
      this.operands = operands;
    }

    public FilteredDimensionSpacesType getFilteredDimensionSpacesType()
    {
      return filteredDimensionSpacesType;
    }

    public List<String> getOperands()
    {
      return operands;
    }
  }

  public enum FilteredDimensionSpacesType
  {
    PREFIX_FILTER,
    LIST_FILTER,
    NOT_IN_LIST_FILTER,
    REGEX_FILTER;

    @Nullable
    public static FilteredDimensionSpacesType fromString(String name)
    {
      return name == null ? null : valueOf(StringUtils.toUpperCase(name));
    }
  }

  public SimpleExtraction(String column, @Nullable ExtractionFn extractionFn)
  {
    this.column = Preconditions.checkNotNull(column, "column");
    this.extractionFn = extractionFn;
    this.fdsOperands = null;
  }

  public SimpleExtraction(String column, @Nullable ExtractionFn extractionFn, @Nullable FilteredDimensionSpacesOperands operands)
  {
    this.column = Preconditions.checkNotNull(column, "column");
    this.extractionFn = extractionFn;
    this.fdsOperands = operands;
  }

  public static SimpleExtraction of(String column, @Nullable ExtractionFn extractionFn)
  {
    return new SimpleExtraction(column, extractionFn);
  }

  public SimpleExtraction toFDSOExtraction(String fdsoFuncName, List<String> operands)
  {
    return new SimpleExtraction(column, extractionFn, new FilteredDimensionSpacesOperands(fdsoFuncName, operands));
  }

  public String getColumn()
  {
    return column;
  }

  @Nullable
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Nullable
  public FilteredDimensionSpacesOperands getFdsOperands()
  {
    return fdsOperands;
  }

  public SimpleExtraction cascade(final ExtractionFn nextExtractionFn)
  {
    return new SimpleExtraction(
        column,
        ExtractionFns.cascade(extractionFn, Preconditions.checkNotNull(nextExtractionFn, "nextExtractionFn"))
    );
  }

  public DimensionSpec toDimensionSpec(
      final String outputName,
      final ValueType outputType
  )
  {
    Preconditions.checkNotNull(outputType, "outputType");
    if (extractionFn == null) {
      if (fdsOperands == null) {
        return new DefaultDimensionSpec(column, outputName, outputType);
      }
      DefaultDimensionSpec defaultDimensionSpec = new DefaultDimensionSpec(column, outputName, outputType);
      switch (fdsOperands.getFilteredDimensionSpacesType()) {
        case LIST_FILTER:
          return new ListFilteredDimensionSpec(defaultDimensionSpec, Sets.newHashSet(fdsOperands.getOperands()), true);
        case NOT_IN_LIST_FILTER:
          return new ListFilteredDimensionSpec(defaultDimensionSpec, Sets.newHashSet(fdsOperands.getOperands()), false);
        case REGEX_FILTER:
          return new RegexFilteredDimensionSpec(defaultDimensionSpec, fdsOperands.getOperands().get(0));
        case PREFIX_FILTER:
          return new PrefixFilteredDimensionSpec(defaultDimensionSpec, fdsOperands.getOperands().get(0));
        default:
          throw new RE("");
      }
    } else {
      return new ExtractionDimensionSpec(column, outputName, outputType, extractionFn);
    }
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
    SimpleExtraction that = (SimpleExtraction) o;
    return column.equals(that.column) &&
           Objects.equals(extractionFn, that.extractionFn);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(column, extractionFn);
  }

  @Override
  public String toString()
  {
    if (extractionFn != null) {
      return StringUtils.format("%s(%s)", extractionFn, column);
    } else {
      return column;
    }
  }
}
