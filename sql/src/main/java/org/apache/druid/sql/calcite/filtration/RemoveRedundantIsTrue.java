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

import com.google.common.base.Function;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.IsTrueDimFilter;
import org.apache.druid.query.filter.OrDimFilter;

import java.util.ArrayList;
import java.util.List;

/**
 * Similar to {@link BottomUpTransform} except only removes redundant IS TRUE filters that are not inside of a NOT
 * filter. The planner leaves behind stuff like `(x == y) IS TRUE` which is a pointless delegate when not living inside
 * of a not filter to enforce proper three-value logic
 */
public class RemoveRedundantIsTrue implements Function<Filtration, Filtration>
{
  private static final RemoveRedundantIsTrue INSTANCE = new RemoveRedundantIsTrue();

  public static RemoveRedundantIsTrue instance()
  {
    return INSTANCE;
  }

  @Override
  public Filtration apply(Filtration filtration)
  {
    if (filtration.getDimFilter() != null) {
      final Filtration retVal = Filtration.create(apply0(filtration.getDimFilter()), filtration.getIntervals());
      return filtration.equals(retVal) ? retVal : apply(retVal);
    } else {
      return filtration;
    }
  }

  private DimFilter apply0(final DimFilter filter)
  {
    // check for AND, OR to process their children, and unwrap any IS TRUE not living under a NOT, anything else we
    // leave alone
    if (filter instanceof AndDimFilter) {
      final List<DimFilter> oldFilters = ((AndDimFilter) filter).getFields();
      final List<DimFilter> newFilters = new ArrayList<>();
      for (DimFilter oldFilter : oldFilters) {
        final DimFilter newFilter = apply0(oldFilter);
        if (newFilter != null) {
          newFilters.add(newFilter);
        }
      }
      if (!newFilters.equals(oldFilters)) {
        return new AndDimFilter(newFilters);
      } else {
        return filter;
      }
    } else if (filter instanceof OrDimFilter) {
      final List<DimFilter> oldFilters = ((OrDimFilter) filter).getFields();
      final List<DimFilter> newFilters = new ArrayList<>();
      for (DimFilter oldFilter : oldFilters) {
        final DimFilter newFilter = apply0(oldFilter);
        if (newFilter != null) {
          newFilters.add(newFilter);
        }
      }
      if (!newFilters.equals(oldFilters)) {
        return new OrDimFilter(newFilters);
      } else {
        return filter;
      }
    } else if (filter instanceof IsTrueDimFilter) {
      final DimFilter oldFilter = ((IsTrueDimFilter) filter).getField();
      final DimFilter newFilter = apply0(oldFilter);
      if (!oldFilter.equals(newFilter)) {
        return newFilter;
      } else {
        return oldFilter;
      }
    } else {
      return filter;
    }
  }
}
