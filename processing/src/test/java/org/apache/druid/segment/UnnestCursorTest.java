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

import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

public class UnnestCursorTest extends InitializedNullHandlingTest
{
  private static String OUTPUT_NAME = "unnested-column";
  private static LinkedHashSet<String> IGNORE_SET = null;
  private static LinkedHashSet<String> IGNORE_SET1 = new LinkedHashSet<>(Arrays.asList("a", "f"));


  @Test
  public void test_list_unnest_cursors()
  {
    ListCursor listCursor = new ListCursor();
    UnnestCursor unnestCursor = new UnnestCursor(listCursor, "dummy", OUTPUT_NAME, IGNORE_SET);
    ColumnValueSelector unnestColumnValueSelector = unnestCursor.getColumnSelectorFactory()
                                                                .makeColumnValueSelector(OUTPUT_NAME);
    int j = 0;
    while (!unnestCursor.isDone()) {
      Object colSelectorVal = unnestColumnValueSelector.getObject();
      Assert.assertEquals(colSelectorVal.toString(), String.valueOf(j));
      j++;
      unnestCursor.advance();
    }
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
    UnnestCursor unnestCursor = new UnnestCursor(listCursor, "dummy", OUTPUT_NAME, IGNORE_SET);
    ColumnValueSelector unnestColumnValueSelector = unnestCursor.getColumnSelectorFactory()
                                                                .makeColumnValueSelector(OUTPUT_NAME);
    int k = 0;
    while (!unnestCursor.isDone()) {
      Object valueSelectorVal = unnestColumnValueSelector.getObject();
      Assert.assertEquals(valueSelectorVal.toString(), expectedResults.get(k));
      k++;
      unnestCursor.advance();
    }
    //Assert.assertEquals(k, 9);
  }

  @Test
  public void test_list_unnest_cursors_user_supplied_list_nulls()
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
    UnnestCursor unnestCursor = new UnnestCursor(listCursor, "dummy", OUTPUT_NAME, IGNORE_SET);
    ColumnValueSelector unnestColumnValueSelector = unnestCursor.getColumnSelectorFactory()
                                                                .makeColumnValueSelector(OUTPUT_NAME);
    int k = 0;
    while (!unnestCursor.isDone()) {
      Object valueSelectorVal = unnestColumnValueSelector.getObject();
      Assert.assertNull(valueSelectorVal);
      //Assert.assertEquals(valueSelectorVal.toString(), expectedResults.get(k));
      k++;
      unnestCursor.advance();
    }
    Assert.assertEquals(k, 4);
  }

  @Test
  public void test_list_unnest_cursors_user_supplied_list_mixed()
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
    UnnestCursor unnestCursor = new UnnestCursor(listCursor, "dummy", OUTPUT_NAME, IGNORE_SET);
    ColumnValueSelector unnestColumnValueSelector = unnestCursor.getColumnSelectorFactory()
                                                                .makeColumnValueSelector(OUTPUT_NAME);
    int k = 0;
    while (!unnestCursor.isDone()) {
      Object valueSelectorVal = unnestColumnValueSelector.getObject();
      if (valueSelectorVal == null) {
        //Assert.assertEquals(null, expectedResults.get(k));
      } else {
        //Assert.assertEquals(valueSelectorVal.toString(), expectedResults.get(k));
      }
      k++;
      unnestCursor.advance();
    }
    Assert.assertEquals(k, 8);
  }

  @Test
  public void test_list_unnest_cursors_user_supplied_strings()
  {
    List<Object> inputList = Arrays.asList("a", "b", "c", "e", "f", "g", "h", "i", "j");

    List<String> expectedResults = Arrays.asList("a", "b", "c", "e", "f", "g", "h", "i", "j");

    //Create base cursor
    ListCursor listCursor = new ListCursor(inputList);

    //Create unnest cursor
    UnnestCursor unnestCursor = new UnnestCursor(listCursor, "dummy", OUTPUT_NAME, IGNORE_SET);
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
  public void test_list_unnest_cursors_user_supplied_strings_list_mixed()
  {
    List<Object> inputList = Arrays.asList("a", "b", "c", "e", "f", Arrays.asList("g", "h"), "i", "j");

    List<String> expectedResults = Arrays.asList("a", "b", "c", "e", "f", "g", "h", "i", "j");

    //Create base cursor
    ListCursor listCursor = new ListCursor(inputList);

    //Create unnest cursor
    UnnestCursor unnestCursor = new UnnestCursor(listCursor, "dummy", OUTPUT_NAME, IGNORE_SET);
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
  public void test_list_unnest_cursors_user_supplied_list_three_level_arrays()
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
    UnnestCursor unnestCursor = new UnnestCursor(listCursor, "dummy", OUTPUT_NAME, IGNORE_SET);
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
    UnnestCursor unnestCursor = new UnnestCursor(listCursor, "dummy", OUTPUT_NAME, IGNORE_SET);
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
    UnnestCursor unnestCursor = new UnnestCursor(listCursor, "dummy", OUTPUT_NAME, IGNORE_SET);
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
  public void test_list_unnest_cursors_user_supplied_list_with_dims()
  {
    List<Object> inputList = Arrays.asList(
        Arrays.asList("a", "b", "c"),
        Arrays.asList("e", "f", "g", "h", "i", null),
        Collections.singletonList("j")
    );

    //Create base cursor
    ListCursor listCursor = new ListCursor(inputList);

    //Create unnest cursor
    UnnestCursor unnestCursor = new UnnestCursor(listCursor, "dummy", OUTPUT_NAME, IGNORE_SET);
    DimensionSelector unnestDimSelector = unnestCursor.getColumnSelectorFactory().makeDimensionSelector(
        DefaultDimensionSpec.of("dummy"));
    ColumnValueSelector unnestColumnValueSelector = unnestCursor.getColumnSelectorFactory()
                                                                .makeColumnValueSelector(OUTPUT_NAME);
    int k = 0;
    while (!unnestCursor.isDone()) {
      Object valueSelectorVal = unnestColumnValueSelector.getObject();
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

    List<String> expectedResults = Arrays.asList("a", "f");

    //Create base cursor
    ListCursor listCursor = new ListCursor(inputList);

    //Create unnest cursor
    UnnestCursor unnestCursor = new UnnestCursor(listCursor, "dummy", OUTPUT_NAME, IGNORE_SET1);
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
}

