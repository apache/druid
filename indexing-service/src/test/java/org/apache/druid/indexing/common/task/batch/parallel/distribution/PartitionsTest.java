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

public class PartitionsTest
{
  private Partitions target;
  private String[] values;

  @Before
  public void setup()
  {
    values = new String[]{"a", "b"};
    target = new Partitions(values);
  }

  @Test
  public void hasCorrectValues()
  {
    Assert.assertEquals(Arrays.asList(values), target);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void isImmutable()
  {
    target.add("should fail");
  }

  @Test
  public void cannotBeIndirectlyModified()
  {
    String[] originalValues = Arrays.copyOf(values, values.length);
    values[0] = "changed";
    Assert.assertEquals(Arrays.asList(originalValues), target);
    Assert.assertNotEquals(Arrays.asList(values), target);
  }

  @Test
  public void serializesDeserializes()
  {
    TestHelper.testSerializesDeserializes(TestHelper.JSON_MAPPER, target);
  }
}
