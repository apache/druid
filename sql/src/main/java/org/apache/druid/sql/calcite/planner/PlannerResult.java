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

package org.apache.druid.sql.calcite.planner;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.java.util.common.guava.Sequence;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class PlannerResult
{
  private final Supplier<Sequence<Object[]>> resultsSupplier;
  private final RelDataType rowType;
  private final Set<String> datasourceNames;
  private final AtomicBoolean didRun = new AtomicBoolean();

  public PlannerResult(
      final Supplier<Sequence<Object[]>> resultsSupplier,
      final RelDataType rowType,
      final Set<String> datasourceNames
  )
  {
    this.resultsSupplier = resultsSupplier;
    this.rowType = rowType;
    this.datasourceNames = ImmutableSet.copyOf(datasourceNames);
  }

  public Sequence<Object[]> run()
  {
    if (!didRun.compareAndSet(false, true)) {
      // Safety check.
      throw new IllegalStateException("Cannot run more than once");
    }
    return resultsSupplier.get();
  }

  public RelDataType rowType()
  {
    return rowType;
  }

  public Set<String> datasourceNames()
  {
    return datasourceNames;
  }
}
