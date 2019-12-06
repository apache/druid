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

package org.apache.druid.indexing.common.task.batch.parallel.distribution;

import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PartitionBoundariesTest
{
  private PartitionBoundaries target;
  private String[] values;
  private List<String> expected;

  @Before
  public void setup()
  {
    values = new String[]{"a", "dup", "dup", "z"};
    expected = Arrays.asList(null, "dup", "z", null);
    target = new PartitionBoundaries(values);
  }

  @Test
  public void hasCorrectValues()
  {
    Assert.assertEquals(expected, target);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void isImmutable()
  {
    target.add("should fail");
  }

  @Test
  public void cannotBeIndirectlyModified()
  {
    values[1] = "changed";
    Assert.assertEquals(expected, target);
  }

  @Test
  public void handlesNoValues()
  {
    Assert.assertEquals(Collections.emptyList(), new PartitionBoundaries());
  }

  @Test
  public void handlesRepeatedValue()
  {
    Assert.assertEquals(Arrays.asList(null, null), new PartitionBoundaries("a", "a", "a"));
  }

  @Test
  public void serializesDeserializes()
  {
    TestHelper.testSerializesDeserializes(TestHelper.JSON_MAPPER, target);
  }
}
