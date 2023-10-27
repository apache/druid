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
import org.junit.Test;

import java.util.Arrays;

public class NaivePartitioningOperatorFactoryTest
{
  @Test
  public void testEquals()
  {
    new EqualsTester()
        .addEqualityGroup(
            naivePartitioningOperator(),
            new NaivePartitioningOperatorFactory(null))
        .addEqualityGroup(
            naivePartitioningOperator("a"),
            naivePartitioningOperator("a"))
        .addEqualityGroup(
            naivePartitioningOperator("b"))
        .addEqualityGroup(
            naivePartitioningOperator("a", "b"))
        .addEqualityGroup(
            naivePartitioningOperator("b", "a"))
        .testEquals();
  }

  private Object naivePartitioningOperator(String... columns)
  {
    return new NaivePartitioningOperatorFactory(Arrays.asList(columns));
  }
}
