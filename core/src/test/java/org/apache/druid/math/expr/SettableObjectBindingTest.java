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

package org.apache.druid.math.expr;


import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

public class SettableObjectBindingTest
{
  @Test
  public void testSettableBinding()
  {
    SettableObjectBinding binding = new SettableObjectBinding(10);
    Assert.assertEquals(0, binding.asMap().size());
    binding = binding.withBinding("x", ExpressionType.LONG);
    Assert.assertEquals(ExpressionType.LONG, binding.get("x"));
    Assert.assertNull(binding.get("y"));
    Assert.assertEquals(ImmutableMap.of("x", ExpressionType.LONG), binding.asMap());
  }
}
