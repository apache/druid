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

import org.apache.druid.error.DruidException;
import org.apache.druid.timeline.DataSegment;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * {@link SegmentPruner} implementation that applies a set of {@link SegmentPruner} against each {@link DataSegment}
 * and will return false for {@link #include(DataSegment)} if ANY pruner indicates that it should not be included.
 */
public class CompositeSegmentPruner implements SegmentPruner
{
  private final Set<SegmentPruner> pruners;

  public CompositeSegmentPruner(Set<SegmentPruner> pruners)
  {
    this.pruners = pruners;
    validate(pruners);
  }

  @Override
  public boolean include(DataSegment segment)
  {
    for (SegmentPruner pruner : pruners) {
      if (!pruner.include(segment)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public SegmentPruner combine(SegmentPruner other)
  {
    final Set<SegmentPruner> combinedPruners;
    if (other instanceof CompositeSegmentPruner composite) {
      // combine both sets, folding filter pruners in or adding if not
      Set<SegmentPruner> combining = new LinkedHashSet<>(pruners);
      for (SegmentPruner pruner : composite.pruners) {
        if (pruner instanceof FilterSegmentPruner filter) {
          combining = foldFilterPruner(combining, filter);
        } else {
          combining.add(pruner);
        }
      }
      combinedPruners = combining;
    } else if (other instanceof FilterSegmentPruner filter) {
      // fold the filter pruner into our set
      combinedPruners = foldFilterPruner(pruners, filter);
    } else {
      // default, add the other one to our set
      combinedPruners = new LinkedHashSet<>(pruners);
      combinedPruners.add(other);
    }
    return new CompositeSegmentPruner(combinedPruners);
  }


  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CompositeSegmentPruner that = (CompositeSegmentPruner) o;
    return Objects.equals(pruners, that.pruners);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(pruners);
  }

  @Override
  public String toString()
  {
    return "CompositeSegmentPruner{" +
           "pruners=" + pruners +
           '}';
  }

  private static void validate(Set<SegmentPruner> pruners)
  {
    FilterSegmentPruner filter = null;
    for (SegmentPruner pruner : pruners) {
      if (pruner instanceof FilterSegmentPruner filterPruner) {
        if (filter != null) {
          throw DruidException.defensive(
              "Combine multiple filter pruners prior to creating composite, found[%s] and [%s]",
              filter,
              filterPruner
          );
        }
        filter = filterPruner;
      }
    }
  }

  private static Set<SegmentPruner> foldFilterPruner(Set<SegmentPruner> pruners, FilterSegmentPruner filter)
  {
    Set<SegmentPruner> combinedPruners = new LinkedHashSet<>();
    // if other is a filterPruner, check to see if we contain any filter pruners to combine with it
    // a composite cannot have more than 1 filter pruner
    boolean notCombined = true;
    for (SegmentPruner pruner : pruners) {
      if (pruner instanceof FilterSegmentPruner) {
        combinedPruners.add(filter.combine(pruner));
        notCombined = false;
      } else {
        combinedPruners.add(pruner);
      }
    }
    if (notCombined) {
      combinedPruners.add(filter);
    }
    return combinedPruners;
  }
}
