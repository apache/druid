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
import org.junit.Test;

import java.util.Collections;

public class NaivePartitioningOperatorFactoryTest
{
  @Test
  public void testEquals()
  {
    new EqualsTester()
        .addEqualityGroup(
            new NaivePartitioningOperatorFactory(Collections.emptyList()),
            new NaivePartitioningOperatorFactory(null))
        .addEqualityGroup(
            new NaivePartitioningOperatorFactory(Lists.newArrayList("a")),
            new NaivePartitioningOperatorFactory(Lists.newArrayList("a")))
        .addEqualityGroup(
            new NaivePartitioningOperatorFactory(Lists.newArrayList("b")))
        .addEqualityGroup(
            new NaivePartitioningOperatorFactory(Lists.newArrayList("a", "b")))
        .addEqualityGroup(
            new NaivePartitioningOperatorFactory(Lists.newArrayList("b", "a")))
        .testEquals();
  }
}
