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

package org.apache.druid.math.expr.vector;

import org.apache.druid.math.expr.SettableVectorInputBinding;
import org.apache.druid.query.filter.vector.VectorMatch;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FilteredVectorInputBindingTest extends InitializedNullHandlingTest
{
  private static final int VECTOR_SIZE = 8;
  private static final Object[] STRINGS = new Object[]{
      "0",
      "1",
      "2",
      "3",
      "4",
      "5",
      "6",
      "7"
  };
  private static final long[] LONGS = new long[]{
      0L,
      1L,
      2L,
      3L,
      4L,
      5L,
      6L,
      7L
  };

  private static final double[] DOUBLES = new double[]{
      0.0,
      1.1,
      2.2,
      3.3,
      4.4,
      5.5,
      6.6,
      7.7
  };

  private static final boolean[] NULLS = new boolean[]{
      false,
      false,
      false,
      true,
      false,
      false,
      true,
      false
  };

  private SettableVectorInputBinding baseBinding;

  @BeforeEach
  public void setup()
  {
    baseBinding = new SettableVectorInputBinding(VECTOR_SIZE);
    baseBinding.addString("string", STRINGS);
    baseBinding.addLong("long", LONGS, NULLS);
    baseBinding.addDouble("double", DOUBLES);
  }
  @Test
  public void testFilterString()
  {
    FilteredVectorInputBinding filteredVectorInputBinding = new FilteredVectorInputBinding(VECTOR_SIZE);
    filteredVectorInputBinding.setBindings(baseBinding);
    VectorMatch matchMaker = filteredVectorInputBinding.getVectorMatch();
    int[] selection = matchMaker.getSelection();
    selection[0] = 1;
    selection[1] = 3;
    selection[2] = 4;
    matchMaker.setSelectionSize(3);

    final Object[] strings = filteredVectorInputBinding.getObjectVector("string");
    Assertions.assertEquals(3, filteredVectorInputBinding.getCurrentVectorSize());
    Assertions.assertEquals("1", strings[0]);
    Assertions.assertEquals("3", strings[1]);
    Assertions.assertEquals("4", strings[2]);
  }

  @Test
  public void testFilterLongWithNull()
  {
    FilteredVectorInputBinding filteredVectorInputBinding = new FilteredVectorInputBinding(VECTOR_SIZE);
    filteredVectorInputBinding.setBindings(baseBinding);
    VectorMatch matchMaker = filteredVectorInputBinding.getVectorMatch();
    int[] selection = matchMaker.getSelection();
    selection[0] = 1;
    selection[1] = 3;
    selection[2] = 4;
    matchMaker.setSelectionSize(3);

    long[] longs = filteredVectorInputBinding.getLongVector("long");
    boolean[] nulls = filteredVectorInputBinding.getNullVector("long");
    Assertions.assertEquals(3, filteredVectorInputBinding.getCurrentVectorSize());
    Assertions.assertNotNull(nulls);
    Assertions.assertEquals(1L, longs[0]);
    Assertions.assertEquals(3L, longs[1]);
    Assertions.assertEquals(4L, longs[2]);
    Assertions.assertFalse(nulls[0]);
    Assertions.assertTrue(nulls[1]);
    Assertions.assertFalse(nulls[2]);

    selection[0] = 0;
    selection[1] = 2;
    selection[2] = 5;
    selection[3] = 6;
    selection[4] = 7;
    matchMaker.setSelectionSize(5);

    longs = filteredVectorInputBinding.getLongVector("long");
    nulls = filteredVectorInputBinding.getNullVector("long");
    Assertions.assertEquals(5, filteredVectorInputBinding.getCurrentVectorSize());
    Assertions.assertNotNull(nulls);
    Assertions.assertEquals(0L, longs[0]);
    Assertions.assertEquals(2L, longs[1]);
    Assertions.assertEquals(5L, longs[2]);
    Assertions.assertEquals(6L, longs[3]);
    Assertions.assertEquals(7L, longs[4]);
    Assertions.assertFalse(nulls[0]);
    Assertions.assertFalse(nulls[1]);
    Assertions.assertFalse(nulls[2]);
    Assertions.assertTrue(nulls[3]);
    Assertions.assertFalse(nulls[4]);


    selection[0] = 1;
    selection[1] = 3;
    selection[2] = 4;
    matchMaker.setSelectionSize(3);

    longs = filteredVectorInputBinding.getLongVector("long");
    nulls = filteredVectorInputBinding.getNullVector("long");
    Assertions.assertEquals(3, filteredVectorInputBinding.getCurrentVectorSize());
    Assertions.assertNotNull(nulls);
    Assertions.assertEquals(1L, longs[0]);
    Assertions.assertEquals(3L, longs[1]);
    Assertions.assertEquals(4L, longs[2]);
    Assertions.assertFalse(nulls[0]);
    Assertions.assertTrue(nulls[1]);
    Assertions.assertFalse(nulls[2]);
  }

  @Test
  public void testDoubles()
  {
    FilteredVectorInputBinding filteredVectorInputBinding = new FilteredVectorInputBinding(VECTOR_SIZE);
    filteredVectorInputBinding.setBindings(baseBinding);
    VectorMatch matchMaker = filteredVectorInputBinding.getVectorMatch();
    int[] selection = matchMaker.getSelection();
    selection[0] = 1;
    selection[1] = 3;
    selection[2] = 4;
    matchMaker.setSelectionSize(3);

    double[] doubles = filteredVectorInputBinding.getDoubleVector("double");
    boolean[] nulls = filteredVectorInputBinding.getNullVector("double");
    Assertions.assertEquals(3, filteredVectorInputBinding.getCurrentVectorSize());
    Assertions.assertEquals(1.1, doubles[0], 0.0);
    Assertions.assertEquals(3.3, doubles[1], 0.0);
    Assertions.assertEquals(4.4, doubles[2], 0.0);

    selection[0] = 0;
    selection[1] = 2;
    selection[2] = 5;
    selection[3] = 6;
    selection[4] = 7;
    matchMaker.setSelectionSize(5);

    doubles = filteredVectorInputBinding.getDoubleVector("double");
    nulls = filteredVectorInputBinding.getNullVector("double");
    Assertions.assertEquals(5, filteredVectorInputBinding.getCurrentVectorSize());

    Assertions.assertEquals(0.0, doubles[0], 0.0);
    Assertions.assertEquals(2.2, doubles[1], 0.0);
    Assertions.assertEquals(5.5, doubles[2], 0.0);
    Assertions.assertEquals(6.6, doubles[3], 0.0);
    Assertions.assertEquals(7.7, doubles[4], 0.0);

    selection[0] = 1;
    selection[1] = 3;
    selection[2] = 4;
    matchMaker.setSelectionSize(3);

    doubles = filteredVectorInputBinding.getDoubleVector("double");
    nulls = filteredVectorInputBinding.getNullVector("double");
    Assertions.assertEquals(3, filteredVectorInputBinding.getCurrentVectorSize());

    Assertions.assertEquals(1.1, doubles[0], 0.0);
    Assertions.assertEquals(3.3, doubles[1], 0.0);
    Assertions.assertEquals(4.4, doubles[2], 0.0);
  }
}
