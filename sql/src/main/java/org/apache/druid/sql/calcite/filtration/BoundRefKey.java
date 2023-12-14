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

package org.apache.druid.sql.calcite.filtration;

import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.ordering.StringComparator;

import java.util.Objects;

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

    if (!Objects.equals(dimension, boundRefKey.dimension)) {
      return false;
    }
    if (!Objects.equals(extractionFn, boundRefKey.extractionFn)) {
      return false;
    }
    return Objects.equals(comparator, boundRefKey.comparator);
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
