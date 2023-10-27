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

package org.apache.druid.query.operator;

import com.google.common.testing.EqualsTester;
import org.apache.druid.query.operator.ColumnWithDirection.Direction;
import org.junit.Test;

import java.util.Arrays;

public class NaiveSortOperatorFactoryTest
{
  @Test
  public void testEquals()
  {
    ColumnWithDirection colA = new ColumnWithDirection("a", Direction.ASC);
    ColumnWithDirection colAdesc = new ColumnWithDirection("a", Direction.DESC);
    ColumnWithDirection colB = new ColumnWithDirection("b", Direction.ASC);

    new EqualsTester()
        .addEqualityGroup(
            new NaiveSortOperatorFactory(null))
        .addEqualityGroup(
            naiveSortOperator())
        .addEqualityGroup(
            naiveSortOperator(colA),
            naiveSortOperator(colA))
        .addEqualityGroup(
            naiveSortOperator(colAdesc))
        .addEqualityGroup(
            naiveSortOperator(colB))
        .addEqualityGroup(
            naiveSortOperator(colA, colB))
        .addEqualityGroup(
            naiveSortOperator(colB, colA))
        // invalid cases; but currently allowed ones
        .addEqualityGroup(
            naiveSortOperator((ColumnWithDirection) null))
        .addEqualityGroup(
            naiveSortOperator(colA, colB, colA))
        .testEquals();
  }

  private Object naiveSortOperator(ColumnWithDirection... cols)
  {
    return new NaiveSortOperatorFactory(Arrays.asList(cols));
  }
}
