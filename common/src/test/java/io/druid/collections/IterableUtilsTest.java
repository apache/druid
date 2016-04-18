/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.collections;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 */
public class IterableUtilsTest
{
  @Test
  public void testBasic() throws Exception
  {
    Set[] sets = {
        ImmutableSet.of("A", "B"),
        ImmutableSet.of("1", "2", "3"),
        ImmutableSet.of("x", "y")
    };

    Iterable<List<String>> expected = Sets.cartesianProduct(Arrays.<Set<String>>asList(sets));
    Iterable<String[]> result = IterableUtils.cartesian(String.class, sets);
    for (String[] x : result) {
      System.out.println("> " + Arrays.toString(x));
    }

    Iterator<List<String>> e = expected.iterator();
    Iterator<String[]> r = result.iterator();
    while (e.hasNext() && r.hasNext()) {
      Assert.assertEquals(String.valueOf(e.next()), Arrays.toString(r.next()));
    }
    Assert.assertFalse(e.hasNext() || r.hasNext());
  }
}
