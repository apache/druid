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

package org.apache.druid.segment;

import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

public class UnnestColumnValueSelectorCursorTest extends InitializedNullHandlingTest
{
  private static String OUTPUT_NAME = "unnested-column";
  private static LinkedHashSet<String> IGNORE_SET = null;
  private static LinkedHashSet<String> IGNORE_SET1 = new LinkedHashSet<>(Arrays.asList("b", "f"));


  @Test
  public void test_list_unnest_cursors()
  {
    ArrayList<Object> baseList = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      List<Object> newList = new ArrayList<>();
      for (int j = 0; j < 2; j++) {
        newList.add(String.valueOf(i * 2 + j));
      }
      baseList.add(newList);
    }
    ListCursor listCursor = new ListCursor(baseList);
    UnnestColumnValueSelectorCursor unnestCursor = new UnnestColumnValueSelectorCursor(
        listCursor,
        listCursor.getColumnSelectorFactory(),
        "dummy",
        OUTPUT_NAME,
        IGNORE_SET
    );
    ColumnValueSelector unnestColumnValueSelector = unnestCursor.getColumnSelectorFactory()
                                                                .makeColumnValueSelector(OUTPUT_NAME);
    int j = 0;
    while (!unnestCursor.isDone()) {
      Object colSelectorVal = unnestColumnValueSelector.getObject();
      Assert.assertEquals(colSelectorVal.toString(), String.valueOf(j));
      j++;
      unnestCursor.advance();
    }
    Assert.assertEquals(j, 4);
  }

  @Test
  public void test_list_unnest_cursors_user_supplied_list()
  {
    List<Object> inputList = Arrays.asList(
        Arrays.asList("a", "b", "c"),
        Arrays.asList("e", "f", "g", "h", "i"),
        Collections.singletonList("j")
    );

    List<String> expectedResults = Arrays.asList("a", "b", "c", "e", "f", "g", "h", "i", "j");

    //Create base cursor
    ListCursor listCursor = new ListCursor(inputList);

    //Create unnest cursor
    UnnestColumnValueSelectorCursor unnestCursor = new UnnestColumnValueSelectorCursor(
        listCursor,
        listCursor.getColumnSelectorFactory(),
        "dummy",
        OUTPUT_NAME,
        IGNORE_SET
    );
    ColumnValueSelector unnestColumnValueSelector = unnestCursor.getColumnSelectorFactory()
                                                                .makeColumnValueSelector(OUTPUT_NAME);
    int k = 0;
    while (!unnestCursor.isDone()) {
      Object valueSelectorVal = unnestColumnValueSelector.getObject();
      Assert.assertEquals(valueSelectorVal.toString(), expectedResults.get(k));
      k++;
      unnestCursor.advance();
    }
    Assert.assertEquals(k, 9);
  }

  @Test
  public void test_list_unnest_cursors_user_supplied_list_only_nulls()
  {
    List<Object> inputList = Arrays.asList(
        Collections.singletonList(null),
        Arrays.asList(null, null),
        Collections.singletonList(null)
    );

    List<String> expectedResults = Arrays.asList(null, null, null, null);

    //Create base cursor
    ListCursor listCursor = new ListCursor(inputList);

    //Create unnest cursor
    UnnestColumnValueSelectorCursor unnestCursor = new UnnestColumnValueSelectorCursor(
        listCursor,
        listCursor.getColumnSelectorFactory(),
        "dummy",
        OUTPUT_NAME,
        IGNORE_SET
    );
    ColumnValueSelector unnestColumnValueSelector = unnestCursor.getColumnSelectorFactory()
                                                                .makeColumnValueSelector(OUTPUT_NAME);
    int k = 0;
    while (!unnestCursor.isDone()) {
      Object valueSelectorVal = unnestColumnValueSelector.getObject();
      Assert.assertNull(valueSelectorVal);
      k++;
      unnestCursor.advance();
    }
    Assert.assertEquals(k, 4);
  }

  @Test
  public void test_list_unnest_cursors_user_supplied_list_mixed_with_nulls()
  {
    List<Object> inputList = Arrays.asList(
        Arrays.asList("a", "b"),
        Arrays.asList("b", "c"),
        "d",
        null,
        null,
        null
    );

    List<String> expectedResults = Arrays.asList("a", "b", "b", "c", "d", null, null, null);

    //Create base cursor
    ListCursor listCursor = new ListCursor(inputList);

    //Create unnest cursor
    UnnestColumnValueSelectorCursor unnestCursor = new UnnestColumnValueSelectorCursor(
        listCursor,
        listCursor.getColumnSelectorFactory(),
        "dummy",
        OUTPUT_NAME,
        IGNORE_SET
    );
    ColumnValueSelector unnestColumnValueSelector = unnestCursor.getColumnSelectorFactory()
                                                                .makeColumnValueSelector(OUTPUT_NAME);
    int k = 0;
    while (!unnestCursor.isDone()) {
      Object valueSelectorVal = unnestColumnValueSelector.getObject();
      if (valueSelectorVal == null) {
        Assert.assertEquals(null, expectedResults.get(k));
      } else {
        Assert.assertEquals(valueSelectorVal.toString(), expectedResults.get(k));
      }
      k++;
      unnestCursor.advance();
    }
    Assert.assertEquals(k, 8);
  }

  @Test
  public void test_list_unnest_cursors_user_supplied_strings_and_no_lists()
  {
    List<Object> inputList = Arrays.asList("a", "b", "c", "e", "f", "g", "h", "i", "j");

    List<String> expectedResults = Arrays.asList("a", "b", "c", "e", "f", "g", "h", "i", "j");

    //Create base cursor
    ListCursor listCursor = new ListCursor(inputList);

    //Create unnest cursor
    UnnestColumnValueSelectorCursor unnestCursor = new UnnestColumnValueSelectorCursor(
        listCursor,
        listCursor.getColumnSelectorFactory(),
        "dummy",
        OUTPUT_NAME,
        IGNORE_SET
    );
    ColumnValueSelector unnestColumnValueSelector = unnestCursor.getColumnSelectorFactory()
                                                                .makeColumnValueSelector(OUTPUT_NAME);
    int k = 0;
    while (!unnestCursor.isDone()) {
      Object valueSelectorVal = unnestColumnValueSelector.getObject();
      Assert.assertEquals(valueSelectorVal.toString(), expectedResults.get(k));
      k++;
      unnestCursor.advance();
    }
    Assert.assertEquals(k, 9);
  }

  @Test
  public void test_list_unnest_cursors_user_supplied_strings_mixed_with_list()
  {
    List<Object> inputList = Arrays.asList("a", "b", "c", "e", "f", Arrays.asList("g", "h"), "i", "j");

    List<String> expectedResults = Arrays.asList("a", "b", "c", "e", "f", "g", "h", "i", "j");

    //Create base cursor
    ListCursor listCursor = new ListCursor(inputList);

    //Create unnest cursor
    UnnestColumnValueSelectorCursor unnestCursor = new UnnestColumnValueSelectorCursor(
        listCursor,
        listCursor.getColumnSelectorFactory(),
        "dummy",
        OUTPUT_NAME,
        IGNORE_SET
    );
    ColumnValueSelector unnestColumnValueSelector = unnestCursor.getColumnSelectorFactory()
                                                                .makeColumnValueSelector(OUTPUT_NAME);
    int k = 0;
    while (!unnestCursor.isDone()) {
      Object valueSelectorVal = unnestColumnValueSelector.getObject();
      Assert.assertEquals(valueSelectorVal.toString(), expectedResults.get(k));
      k++;
      unnestCursor.advance();
    }
    Assert.assertEquals(k, 9);
  }

  @Test
  public void test_list_unnest_cursors_user_supplied_lists_three_levels()
  {
    List<Object> inputList = Arrays.asList(
        Arrays.asList("a", "b", "c"),
        Arrays.asList("e", "f", "g", "h", "i"),
        Arrays.asList("j", Arrays.asList("a", "b"))
    );

    List<Object> expectedResults = Arrays.asList("a", "b", "c", "e", "f", "g", "h", "i", "j", Arrays.asList("a", "b"));

    //Create base cursor
    ListCursor listCursor = new ListCursor(inputList);

    //Create unnest cursor
    UnnestColumnValueSelectorCursor unnestCursor = new UnnestColumnValueSelectorCursor(
        listCursor,
        listCursor.getColumnSelectorFactory(),
        "dummy",
        OUTPUT_NAME,
        IGNORE_SET
    );
    ColumnValueSelector unnestColumnValueSelector = unnestCursor.getColumnSelectorFactory()
                                                                .makeColumnValueSelector(OUTPUT_NAME);
    int k = 0;
    while (!unnestCursor.isDone()) {
      Object valueSelectorVal = unnestColumnValueSelector.getObject();
      Assert.assertEquals(valueSelectorVal.toString(), expectedResults.get(k).toString());
      k++;
      unnestCursor.advance();
    }
    Assert.assertEquals(k, 10);
  }

  @Test
  public void test_list_unnest_of_unnest_cursors_user_supplied_list_three_levels()
  {
    List<Object> inputList = Arrays.asList(
        Arrays.asList("a", "b", "c"),
        Arrays.asList("e", "f", "g", "h", "i"),
        Arrays.asList("j", Arrays.asList("a", "b"))
    );

    List<Object> expectedResults = Arrays.asList("a", "b", "c", "e", "f", "g", "h", "i", "j", "a", "b");

    //Create base cursor
    ListCursor listCursor = new ListCursor(inputList);

    //Create unnest cursor
    UnnestColumnValueSelectorCursor childCursor = new UnnestColumnValueSelectorCursor(
        listCursor,
        listCursor.getColumnSelectorFactory(),
        "dummy",
        OUTPUT_NAME,
        IGNORE_SET
    );
    UnnestColumnValueSelectorCursor parentCursor = new UnnestColumnValueSelectorCursor(
        childCursor,
        childCursor.getColumnSelectorFactory(),
        OUTPUT_NAME,
        "tmp-out",
        IGNORE_SET
    );
    ColumnValueSelector unnestColumnValueSelector = parentCursor.getColumnSelectorFactory()
                                                                .makeColumnValueSelector("tmp-out");
    int k = 0;
    while (!parentCursor.isDone()) {
      Object valueSelectorVal = unnestColumnValueSelector.getObject();
      Assert.assertEquals(valueSelectorVal.toString(), expectedResults.get(k).toString());
      k++;
      parentCursor.advance();
    }
    Assert.assertEquals(k, 11);
  }

  @Test
  public void test_list_unnest_cursors_user_supplied_list_with_nulls()
  {
    List<Object> inputList = Arrays.asList(
        Arrays.asList("a", "b", "c"),
        Arrays.asList("e", "f", "g", "h", "i", null),
        Collections.singletonList("j")
    );

    List<Object> expectedResults = Arrays.asList("a", "b", "c", "e", "f", "g", "h", "i", null, "j");


    //Create base cursor
    ListCursor listCursor = new ListCursor(inputList);

    //Create unnest cursor
    UnnestColumnValueSelectorCursor unnestCursor = new UnnestColumnValueSelectorCursor(
        listCursor,
        listCursor.getColumnSelectorFactory(),
        "dummy",
        OUTPUT_NAME,
        IGNORE_SET
    );
    ColumnValueSelector unnestColumnValueSelector = unnestCursor.getColumnSelectorFactory()
                                                                .makeColumnValueSelector(OUTPUT_NAME);
    int k = 0;
    while (!unnestCursor.isDone()) {
      Object valueSelectorVal = unnestColumnValueSelector.getObject();
      if (valueSelectorVal == null) {
        Assert.assertEquals(null, expectedResults.get(k));
      } else {
        Assert.assertEquals(valueSelectorVal.toString(), expectedResults.get(k));
      }
      k++;
      unnestCursor.advance();
    }
    Assert.assertEquals(k, expectedResults.size());
  }

  @Test
  public void test_list_unnest_cursors_user_supplied_list_with_dups()
  {
    List<Object> inputList = Arrays.asList(
        Arrays.asList("a", "a", "a"),
        Arrays.asList("e", "f", null, "h", "i", null),
        Collections.singletonList("j")
    );

    List<Object> expectedResults = Arrays.asList("a", "a", "a", "e", "f", null, "h", "i", null, "j");

    //Create base cursor
    ListCursor listCursor = new ListCursor(inputList);

    //Create unnest cursor
    UnnestColumnValueSelectorCursor unnestCursor = new UnnestColumnValueSelectorCursor(
        listCursor,
        listCursor.getColumnSelectorFactory(),
        "dummy",
        OUTPUT_NAME,
        IGNORE_SET
    );
    ColumnValueSelector unnestColumnValueSelector = unnestCursor.getColumnSelectorFactory()
                                                                .makeColumnValueSelector(OUTPUT_NAME);
    int k = 0;
    while (!unnestCursor.isDone()) {
      Object valueSelectorVal = unnestColumnValueSelector.getObject();
      if (valueSelectorVal == null) {
        Assert.assertEquals(null, expectedResults.get(k));
      } else {
        Assert.assertEquals(valueSelectorVal.toString(), expectedResults.get(k));
      }
      k++;
      unnestCursor.advance();
    }
    Assert.assertEquals(k, 10);
  }

  @Test
  public void test_list_unnest_cursors_user_supplied_list_with_ignore_set()
  {
    List<Object> inputList = Arrays.asList(
        Arrays.asList("a", "b", "c"),
        Arrays.asList("e", "f", "g", "h", "i"),
        Collections.singletonList("j")
    );

    List<String> expectedResults = Arrays.asList("b", "f");

    //Create base cursor
    ListCursor listCursor = new ListCursor(inputList);

    //Create unnest cursor
    UnnestColumnValueSelectorCursor unnestCursor = new UnnestColumnValueSelectorCursor(
        listCursor,
        listCursor.getColumnSelectorFactory(),
        "dummy",
        OUTPUT_NAME,
        IGNORE_SET1
    );
    ColumnValueSelector unnestColumnValueSelector = unnestCursor.getColumnSelectorFactory()
                                                                .makeColumnValueSelector(OUTPUT_NAME);
    int k = 0;
    while (!unnestCursor.isDone()) {
      Object valueSelectorVal = unnestColumnValueSelector.getObject();
      if (valueSelectorVal == null) {
        Assert.assertEquals(null, expectedResults.get(k));
      } else {
        Assert.assertEquals(valueSelectorVal.toString(), expectedResults.get(k));
      }
      k++;
      unnestCursor.advance();
    }
    Assert.assertEquals(k, 2);
  }

  @Test
  public void test_list_unnest_cursors_user_supplied_list_double()
  {
    List<Object> inputList = Arrays.asList(
        Arrays.asList(1, 2, 3),
        Arrays.asList(4, 5, 6, 7, 8),
        Collections.singletonList(9)
    );

    List<Double> expectedResults = Arrays.asList(1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d);

    //Create base cursor
    ListCursor listCursor = new ListCursor(inputList);

    //Create unnest cursor
    UnnestColumnValueSelectorCursor unnestCursor = new UnnestColumnValueSelectorCursor(
        listCursor,
        listCursor.getColumnSelectorFactory(),
        "dummy",
        OUTPUT_NAME,
        IGNORE_SET
    );
    ColumnValueSelector unnestColumnValueSelector = unnestCursor.getColumnSelectorFactory()
                                                                .makeColumnValueSelector(OUTPUT_NAME);
    int k = 0;
    while (!unnestCursor.isDone()) {
      Double valueSelectorVal = unnestColumnValueSelector.getDouble();
      Assert.assertEquals(valueSelectorVal, expectedResults.get(k));
      k++;
      unnestCursor.advance();
    }
    Assert.assertEquals(k, 9);
  }

  @Test
  public void test_list_unnest_cursors_user_supplied_list_float()
  {
    List<Object> inputList = Arrays.asList(
        Arrays.asList(1, 2, 3),
        Arrays.asList(4, 5, 6, 7, 8),
        Collections.singletonList(9)
    );

    List<Float> expectedResults = Arrays.asList(1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f);

    //Create base cursor
    ListCursor listCursor = new ListCursor(inputList);

    //Create unnest cursor
    UnnestColumnValueSelectorCursor unnestCursor = new UnnestColumnValueSelectorCursor(
        listCursor,
        listCursor.getColumnSelectorFactory(),
        "dummy",
        OUTPUT_NAME,
        IGNORE_SET
    );
    ColumnValueSelector unnestColumnValueSelector = unnestCursor.getColumnSelectorFactory()
                                                                .makeColumnValueSelector(OUTPUT_NAME);
    int k = 0;
    while (!unnestCursor.isDone()) {
      Float valueSelectorVal = unnestColumnValueSelector.getFloat();
      Assert.assertEquals(valueSelectorVal, expectedResults.get(k));
      k++;
      unnestCursor.advance();
    }
    Assert.assertEquals(k, 9);
  }

  @Test
  public void test_list_unnest_cursors_user_supplied_list_long()
  {
    List<Object> inputList = Arrays.asList(
        Arrays.asList(1, 2, 3),
        Arrays.asList(4, 5, 6, 7, 8),
        Collections.singletonList(9)
    );

    List<Long> expectedResults = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);

    //Create base cursor
    ListCursor listCursor = new ListCursor(inputList);

    //Create unnest cursor
    UnnestColumnValueSelectorCursor unnestCursor = new UnnestColumnValueSelectorCursor(
        listCursor,
        listCursor.getColumnSelectorFactory(),
        "dummy",
        OUTPUT_NAME,
        IGNORE_SET
    );
    ColumnValueSelector unnestColumnValueSelector = unnestCursor.getColumnSelectorFactory()
                                                                .makeColumnValueSelector(OUTPUT_NAME);

    int k = 0;
    while (!unnestCursor.isDone()) {
      Object obj = unnestColumnValueSelector.getObject();
      Assert.assertNotNull(obj);
      Long valueSelectorVal = unnestColumnValueSelector.getLong();
      Assert.assertEquals(valueSelectorVal, expectedResults.get(k));
      k++;
      unnestCursor.advance();
    }
    Assert.assertEquals(k, 9);
  }

  @Test
  public void test_list_unnest_cursors_user_supplied_list_three_level_arrays_and_methods()
  {
    List<Object> inputList = Arrays.asList(
        Arrays.asList("a", "b", "c"),
        Arrays.asList("e", "f", "g", "h", "i"),
        Arrays.asList("j", Arrays.asList("a", "b"))
    );

    List<Object> expectedResults = Arrays.asList("a", "b", "c", "e", "f", "g", "h", "i", "j", Arrays.asList("a", "b"));

    //Create base cursor
    ListCursor listCursor = new ListCursor(inputList);

    //Create unnest cursor
    UnnestColumnValueSelectorCursor unnestCursor = new UnnestColumnValueSelectorCursor(
        listCursor,
        listCursor.getColumnSelectorFactory(),
        "dummy",
        OUTPUT_NAME,
        IGNORE_SET
    );
    ColumnValueSelector unnestColumnValueSelector = unnestCursor.getColumnSelectorFactory()
                                                                .makeColumnValueSelector(OUTPUT_NAME);

    int k = 0;
    while (!unnestCursor.isDone()) {
      Object valueSelectorVal = unnestColumnValueSelector.getObject();
      Assert.assertEquals(valueSelectorVal.toString(), expectedResults.get(k).toString());
      k++;
      unnestCursor.advance();
    }
    Assert.assertEquals(k, 10);
    unnestCursor.reset();
    Assert.assertFalse(unnestCursor.isDoneOrInterrupted());
  }

  @Test(expected = UOE.class)
  public void test_list_unnest_cursors_dimSelector()
  {
    List<Object> inputList = Arrays.asList(
        Arrays.asList("a", "b", "c"),
        Arrays.asList("e", "f", "g", "h", "i"),
        Collections.singletonList("j")
    );

    List<String> expectedResults = Arrays.asList("a", "b", "c", "e", "f", "g", "h", "i", "j");

    //Create base cursor
    ListCursor listCursor = new ListCursor(inputList);

    //Create unnest cursor
    UnnestColumnValueSelectorCursor unnestCursor = new UnnestColumnValueSelectorCursor(
        listCursor,
        listCursor.getColumnSelectorFactory(),
        "dummy",
        OUTPUT_NAME,
        IGNORE_SET
    );
    unnestCursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of(OUTPUT_NAME));
  }

  @Test
  public void test_list_unnest_cursors_user_supplied_list_of_integers()
  {
    List<Object> inputList = Arrays.asList(
        Arrays.asList(1, 2, 3),
        Arrays.asList(4, 5, 6, 7, 8),
        Collections.singletonList(9)
    );

    List<Integer> expectedResults = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);

    //Create base cursor
    ListCursor listCursor = new ListCursor(inputList);

    //Create unnest cursor
    UnnestColumnValueSelectorCursor unnestCursor = new UnnestColumnValueSelectorCursor(
        listCursor,
        listCursor.getColumnSelectorFactory(),
        "dummy",
        OUTPUT_NAME,
        IGNORE_SET
    );
    ColumnValueSelector unnestColumnValueSelector = unnestCursor.getColumnSelectorFactory()
                                                                .makeColumnValueSelector(OUTPUT_NAME);
    int k = 0;
    while (!unnestCursor.isDone()) {
      Object valueSelectorVal = unnestColumnValueSelector.getObject();
      Assert.assertEquals(valueSelectorVal.toString(), expectedResults.get(k).toString());
      k++;
      unnestCursor.advance();
    }
    Assert.assertEquals(k, 9);
  }
}

