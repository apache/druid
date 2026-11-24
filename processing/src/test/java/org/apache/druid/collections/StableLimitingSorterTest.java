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

package org.apache.druid.collections;

import com.google.common.collect.Lists;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.List;

public class StableLimitingSorterTest
{
  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(StableLimitingSorter.NumberedElement.class).usingGetClass().verify();
  }

  @Test
  public void testPeekLast()
  {
    // Compare only the first character, so "b1" and "b2" are equal.
    final StableLimitingSorter<String> sorter =
        new StableLimitingSorter<>(Comparator.comparing(s -> s.charAt(0)), 2);
    Assertions.assertNull(sorter.peekLast());

    sorter.add("c");
    Assertions.assertEquals("c", sorter.peekLast());

    sorter.add("b1");
    Assertions.assertEquals("c", sorter.peekLast());

    sorter.add("a");
    Assertions.assertEquals("b1", sorter.peekLast());

    // Equal to the greatest element, but added later, so it is not retained.
    sorter.add("b2");
    Assertions.assertEquals("b1", sorter.peekLast());
    Assertions.assertEquals(List.of("a", "b1"), Lists.newArrayList(sorter.drain()));
  }
}
