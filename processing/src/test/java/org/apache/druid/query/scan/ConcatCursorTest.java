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

package org.apache.druid.query.scan;

import com.google.common.collect.ImmutableList;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.ListCursor;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ConcatCursorTest
{
  @Test
  public void testConcatCursor()
  {
    Cursor dummyCursor1 = new ListCursor(new ArrayList<>());
    Cursor cursor1 = new ListCursor(ImmutableList.of("a", "b"));
    Cursor dummyCursor2 = new ListCursor(new ArrayList<>());
    Cursor cursor2 = new ListCursor(ImmutableList.of("c", "d"));
    Cursor dummyCursor3 = new ListCursor(new ArrayList<>());

    Cursor concatCursor = new ConcatCursor(ImmutableList.of(
        dummyCursor1,
        cursor1,
        dummyCursor2,
        cursor2,
        dummyCursor3
    ));

    List<Object> tempList = new ArrayList<>();
    // Initial iteration
    while (!concatCursor.isDone()) {
      tempList.add(concatCursor.getColumnSelectorFactory().makeColumnValueSelector("ignored").getObject());
      concatCursor.advance();
    }
    Assert.assertEquals(ImmutableList.of("a", "b", "c", "d"), tempList);

    // Check if reset() works after exhausting the cursor
    concatCursor.reset();
    tempList.clear();
    for (int i = 0; i < 3; ++i) {
      tempList.add(concatCursor.getColumnSelectorFactory().makeColumnValueSelector("ignored").getObject());
      concatCursor.advance();
    }
    Assert.assertEquals(ImmutableList.of("a", "b", "c"), tempList);

    // Check if reset() works from the middle
    concatCursor.reset();
    tempList.clear();
    while (!concatCursor.isDone()) {
      tempList.add(concatCursor.getColumnSelectorFactory().makeColumnValueSelector("ignored").getObject());
      concatCursor.advance();
    }
    Assert.assertEquals(ImmutableList.of("a", "b", "c", "d"), tempList);
  }

  @Test
  public void testConcatCursorOfEmptyCursors()
  {
    Cursor dummyCursor1 = new ListCursor(new ArrayList<>());
    Cursor dummyCursor2 = new ListCursor(new ArrayList<>());
    Cursor concatCursor = new ConcatCursor(ImmutableList.of(
        dummyCursor1,
        dummyCursor2
    ));
    Assert.assertTrue(concatCursor.isDone());
  }

  @Test
  public void testConcatCursorWhenBeginningCursorIsEmpty()
  {
    Cursor dummyCursor1 = new ListCursor(new ArrayList<>());
    Cursor cursor1 = new ListCursor(ImmutableList.of("a", "b"));
    Cursor concatCursor = new ConcatCursor(ImmutableList.of(
        dummyCursor1,
        cursor1
    ));

    List<Object> tempList = new ArrayList<>();

    while (!concatCursor.isDone()) {
      tempList.add(concatCursor.getColumnSelectorFactory().makeColumnValueSelector("ignored").getObject());
      concatCursor.advance();
    }
    Assert.assertEquals(ImmutableList.of("a", "b"), tempList);

    // Check if reset() works after exhausting the cursor
    concatCursor.reset();
    tempList.clear();
    for (int i = 0; i < 1; ++i) {
      tempList.add(concatCursor.getColumnSelectorFactory().makeColumnValueSelector("ignored").getObject());
      concatCursor.advance();
    }
    Assert.assertEquals(ImmutableList.of("a"), tempList);

    // Check if reset() works from the middle
    concatCursor.reset();
    tempList.clear();
    while (!concatCursor.isDone()) {
      tempList.add(concatCursor.getColumnSelectorFactory().makeColumnValueSelector("ignored").getObject());
      concatCursor.advance();
    }
    Assert.assertEquals(ImmutableList.of("a", "b"), tempList);
  }

  @Test
  public void testConcatCursorWhenEndingCursorIsEmpty()
  {
    Cursor dummyCursor1 = new ListCursor(new ArrayList<>());
    Cursor cursor1 = new ListCursor(ImmutableList.of("a", "b"));
    Cursor concatCursor = new ConcatCursor(ImmutableList.of(
        cursor1,
        dummyCursor1
    ));

    List<Object> tempList = new ArrayList<>();

    while (!concatCursor.isDone()) {
      tempList.add(concatCursor.getColumnSelectorFactory().makeColumnValueSelector("ignored").getObject());
      concatCursor.advance();
    }
    Assert.assertEquals(ImmutableList.of("a", "b"), tempList);

    // Check if reset() works after exhausting the cursor
    concatCursor.reset();
    tempList.clear();
    for (int i = 0; i < 1; ++i) {
      tempList.add(concatCursor.getColumnSelectorFactory().makeColumnValueSelector("ignored").getObject());
      concatCursor.advance();
    }
    Assert.assertEquals(ImmutableList.of("a"), tempList);

    // Check if reset() works from the middle
    concatCursor.reset();
    tempList.clear();
    while (!concatCursor.isDone()) {
      tempList.add(concatCursor.getColumnSelectorFactory().makeColumnValueSelector("ignored").getObject());
      concatCursor.advance();
    }
    Assert.assertEquals(ImmutableList.of("a", "b"), tempList);
  }

  @Test
  public void testConcatCursorWhenMultipleEmptyCursorsAtBeginning()
  {
    Cursor dummyCursor1 = new ListCursor(new ArrayList<>());
    Cursor dummyCursor2 = new ListCursor(new ArrayList<>());
    Cursor dummyCursor3 = new ListCursor(new ArrayList<>());
    Cursor cursor1 = new ListCursor(ImmutableList.of("a", "b"));
    Cursor concatCursor = new ConcatCursor(ImmutableList.of(
        dummyCursor1,
        dummyCursor2,
        dummyCursor3,
        cursor1
    ));

    List<Object> tempList = new ArrayList<>();

    while (!concatCursor.isDone()) {
      tempList.add(concatCursor.getColumnSelectorFactory().makeColumnValueSelector("ignored").getObject());
      concatCursor.advance();
    }
    Assert.assertEquals(ImmutableList.of("a", "b"), tempList);

    // Check if reset() works after exhausting the cursor
    concatCursor.reset();
    tempList.clear();
    for (int i = 0; i < 1; ++i) {
      tempList.add(concatCursor.getColumnSelectorFactory().makeColumnValueSelector("ignored").getObject());
      concatCursor.advance();
    }
    Assert.assertEquals(ImmutableList.of("a"), tempList);

    // Check if reset() works from the middle
    concatCursor.reset();
    tempList.clear();
    while (!concatCursor.isDone()) {
      tempList.add(concatCursor.getColumnSelectorFactory().makeColumnValueSelector("ignored").getObject());
      concatCursor.advance();
    }
    Assert.assertEquals(ImmutableList.of("a", "b"), tempList);
  }

  @Test
  public void testConcatCursorWhenMultipleEmptyCursorsAtEnd()
  {
    Cursor dummyCursor1 = new ListCursor(new ArrayList<>());
    Cursor dummyCursor2 = new ListCursor(new ArrayList<>());
    Cursor dummyCursor3 = new ListCursor(new ArrayList<>());
    Cursor cursor1 = new ListCursor(ImmutableList.of("a", "b"));
    Cursor concatCursor = new ConcatCursor(ImmutableList.of(
        cursor1,
        dummyCursor1,
        dummyCursor2,
        dummyCursor3
    ));

    List<Object> tempList = new ArrayList<>();

    while (!concatCursor.isDone()) {
      tempList.add(concatCursor.getColumnSelectorFactory().makeColumnValueSelector("ignored").getObject());
      concatCursor.advance();
    }
    Assert.assertEquals(ImmutableList.of("a", "b"), tempList);

    // Check if reset() works after exhausting the cursor
    concatCursor.reset();
    tempList.clear();
    for (int i = 0; i < 1; ++i) {
      tempList.add(concatCursor.getColumnSelectorFactory().makeColumnValueSelector("ignored").getObject());
      concatCursor.advance();
    }
    Assert.assertEquals(ImmutableList.of("a"), tempList);

    // Check if reset() works from the middle
    concatCursor.reset();
    tempList.clear();
    while (!concatCursor.isDone()) {
      tempList.add(concatCursor.getColumnSelectorFactory().makeColumnValueSelector("ignored").getObject());
      concatCursor.advance();
    }
    Assert.assertEquals(ImmutableList.of("a", "b"), tempList);
  }

  @Test
  public void testConcatCursorWhenMultipleEmptyCursorsAtTheMiddle()
  {
    Cursor dummyCursor1 = new ListCursor(new ArrayList<>());
    Cursor dummyCursor2 = new ListCursor(new ArrayList<>());
    Cursor dummyCursor3 = new ListCursor(new ArrayList<>());
    Cursor cursor1 = new ListCursor(ImmutableList.of("a"));
    Cursor cursor2 = new ListCursor(ImmutableList.of("b"));
    Cursor concatCursor = new ConcatCursor(ImmutableList.of(
        cursor1,
        dummyCursor1,
        dummyCursor2,
        dummyCursor3,
        cursor2
    ));

    List<Object> tempList = new ArrayList<>();

    while (!concatCursor.isDone()) {
      tempList.add(concatCursor.getColumnSelectorFactory().makeColumnValueSelector("ignored").getObject());
      concatCursor.advance();
    }
    Assert.assertEquals(ImmutableList.of("a", "b"), tempList);

    // Check if reset() works after exhausting the cursor
    concatCursor.reset();
    tempList.clear();
    for (int i = 0; i < 1; ++i) {
      tempList.add(concatCursor.getColumnSelectorFactory().makeColumnValueSelector("ignored").getObject());
      concatCursor.advance();
    }
    Assert.assertEquals(ImmutableList.of("a"), tempList);

    // Check if reset() works from the middle
    concatCursor.reset();
    tempList.clear();
    while (!concatCursor.isDone()) {
      tempList.add(concatCursor.getColumnSelectorFactory().makeColumnValueSelector("ignored").getObject());
      concatCursor.advance();
    }
    Assert.assertEquals(ImmutableList.of("a", "b"), tempList);
  }
}
