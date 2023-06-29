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

package org.apache.druid.query.rowsandcols.semantic;

import com.google.common.collect.FluentIterable;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.NoAsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumnsTestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.function.Function;

/**
 * This base class exists to provide standard parameterization for Semantic interfaces.  The idea is that the test
 * will be fed a function that can be used to build a RowsAndColumns and then the test should do whatever it
 * needs with the RowsAndColumns.  By extending this base class, the test will end up running against every
 * independent implementation of RowsAndColumns that has been registered with {@link RowsAndColumnsTestBase}.
 */
@RunWith(Parameterized.class)
public abstract class SemanticTestBase
{
  static {
    NullHandling.initializeForTests();
  }

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> parameterFeed()
  {
    return FluentIterable.from(RowsAndColumnsTestBase.makerFeeder())
                         .transformAndConcat(input -> {
                           final String name = ((Class<?>) input[0]).getSimpleName();
                           return Arrays.asList(
                               new Object[]{name, input[1]},
                               new Object[]{"NoAs-" + name, wrapNoAs(input[1])}
                           );
                         });
  }

  private final String name;
  private final Function<MapOfColumnsRowsAndColumns, RowsAndColumns> fn;

  public SemanticTestBase(
      String name,
      Function<MapOfColumnsRowsAndColumns, RowsAndColumns> fn
  )
  {
    this.name = name;
    this.fn = fn;
  }

  public RowsAndColumns make(MapOfColumnsRowsAndColumns rac)
  {
    try {
      return fn.apply(rac);
    }
    catch (RuntimeException e) {
      throw new RE(e, "using name[%s]", name);
    }
  }

  @SuppressWarnings("unchecked")
  private static Function<MapOfColumnsRowsAndColumns, RowsAndColumns> wrapNoAs(Object obj)
  {
    return ((Function<MapOfColumnsRowsAndColumns, RowsAndColumns>) obj).andThen(NoAsRowsAndColumns::new);
  }
}
