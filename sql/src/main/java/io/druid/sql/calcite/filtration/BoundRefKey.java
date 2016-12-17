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

package io.druid.sql.calcite.filtration;

import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.ordering.StringComparator;

public class BoundRefKey
{
  private final String dimension;
  private final ExtractionFn extractionFn;
  private final StringComparator comparator;

  public BoundRefKey(String dimension, ExtractionFn extractionFn, StringComparator comparator)
  {
    this.dimension = dimension;
    this.extractionFn = extractionFn;
    this.comparator = comparator;
  }

  public static BoundRefKey from(BoundDimFilter filter)
  {
    return new BoundRefKey(
        filter.getDimension(),
        filter.getExtractionFn(),
        filter.getOrdering()
    );
  }

  public static BoundRefKey from(SelectorDimFilter filter, StringComparator comparator)
  {
    return new BoundRefKey(
        filter.getDimension(),
        filter.getExtractionFn(),
        comparator
    );
  }

  public String getDimension()
  {
    return dimension;
  }

  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  public StringComparator getComparator()
  {
    return comparator;
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

    BoundRefKey boundRefKey = (BoundRefKey) o;

    if (dimension != null ? !dimension.equals(boundRefKey.dimension) : boundRefKey.dimension != null) {
      return false;
    }
    if (extractionFn != null ? !extractionFn.equals(boundRefKey.extractionFn) : boundRefKey.extractionFn != null) {
      return false;
    }
    return comparator != null ? comparator.equals(boundRefKey.comparator) : boundRefKey.comparator == null;
  }

  @Override
  public int hashCode()
  {
    int result = dimension != null ? dimension.hashCode() : 0;
    result = 31 * result + (extractionFn != null ? extractionFn.hashCode() : 0);
    result = 31 * result + (comparator != null ? comparator.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "BoundRefKey{" +
           "dimension='" + dimension + '\'' +
           ", extractionFn=" + extractionFn +
           ", comparator=" + comparator +
           '}';
  }
}
