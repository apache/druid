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

package org.apache.druid.java.util.common.parsers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class ParserUtilsTest
{
  @Test
  public void testFindDuplicatesMixedCases()
  {
    final List<String> fields = ImmutableList.of("f1", "f2", "F1", "F2", "f3");
    Assert.assertEquals(Collections.emptySet(), ParserUtils.findDuplicates(fields));
  }

  @Test
  public void testFindDuplicates()
  {
    final List<String> fields = ImmutableList.of("f1", "f2", "F1", "F2", "f1", "F2");
    Assert.assertEquals(ImmutableSet.of("f1", "F2"), ParserUtils.findDuplicates(fields));
  }
}
