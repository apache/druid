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

import com.google.common.collect.Lists;
import com.google.common.testing.EqualsTester;
import org.apache.druid.query.operator.ColumnWithDirection.Direction;
import org.junit.Test;

import java.util.Collections;

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
            new NaiveSortOperatorFactory(Collections.emptyList()),
            new NaiveSortOperatorFactory(null))
        .addEqualityGroup(
            new NaiveSortOperatorFactory(Lists.newArrayList(colA)),
            new NaiveSortOperatorFactory(Lists.newArrayList(colA)))
        .addEqualityGroup(
            new NaiveSortOperatorFactory(Lists.newArrayList(colAdesc)))
        .addEqualityGroup(
            new NaiveSortOperatorFactory(Lists.newArrayList(colB)))
        .addEqualityGroup(
            new NaiveSortOperatorFactory(Lists.newArrayList(colA, colB)))
        .addEqualityGroup(
            new NaiveSortOperatorFactory(Lists.newArrayList(colB, colA)))
        // invalid cases; but currently allowed ones
        .addEqualityGroup(
            new NaiveSortOperatorFactory(Lists.newArrayList((ColumnWithDirection) null)))
        .addEqualityGroup(
            new NaiveSortOperatorFactory(Lists.newArrayList(colA, colB, colA)))
        .testEquals();
  }

}
