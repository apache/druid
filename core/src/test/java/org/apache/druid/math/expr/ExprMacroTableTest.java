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

import org.apache.druid.java.util.common.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class ExprMacroTableTest
{
  class MyMacroTable implements ExprMacroTable.ExprMacro
  {
    @Override
    public String name()
    {
      return "myfunc";
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      return null;
    }
  }

  class OtherMacroTable implements ExprMacroTable.ExprMacro
  {
    @Override
    public String name()
    {
      return "otherfunc";
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      return null;
    }
  }

  @Test
  public void testEqualsAndHashCode()
  {
    ExprMacroTable nil1 = ExprMacroTable.nil();
    ExprMacroTable nil2 = ExprMacroTable.nil();

    // two NIL should be equal
    Assert.assertEquals(nil1, nil2);

    List<ExprMacroTable.ExprMacro> macros = new ArrayList<>();
    macros.add(new MyMacroTable());

    List<ExprMacroTable.ExprMacro> otherMacros = new ArrayList<>();
    otherMacros.add(new OtherMacroTable());

    List<ExprMacroTable.ExprMacro> myMacros = new ArrayList<>();
    myMacros.add(new MyMacroTable());

    List<ExprMacroTable.ExprMacro> moreThanOneMacros = new ArrayList<>();
    moreThanOneMacros.add(new MyMacroTable());
    moreThanOneMacros.add(new OtherMacroTable());

    ExprMacroTable myTable = new ExprMacroTable(macros);
    ExprMacroTable otherTable = new ExprMacroTable(otherMacros);
    ExprMacroTable myTable2 = new ExprMacroTable(myMacros);
    ExprMacroTable moreThanOneTable = new ExprMacroTable(moreThanOneMacros);

    // has different macros
    Assert.assertNotEquals(myTable, otherTable);
    // has the same macros
    Assert.assertEquals(myTable, myTable2);
    // the size is not equal
    Assert.assertNotEquals(myTable, moreThanOneTable);

    Map<Pair<String, ExprMacroTable>, Expr> cache = new HashMap();

    Pair<String, ExprMacroTable> key1 = new Pair<>("1", myTable);
    Assert.assertFalse(cache.containsKey(key1));
    LongExpr longExpr = new LongExpr(1L);
    cache.put(key1, longExpr);
    Assert.assertTrue(cache.containsKey(key1));
    Assert.assertEquals(longExpr, cache.get(key1));  // the same object

    DoubleExpr doubleExpr = new DoubleExpr(2.0);
    Pair<String, ExprMacroTable> key2 = new Pair<>("2.0", otherTable);
    Assert.assertFalse(cache.containsKey(key2));
    cache.put(key2, doubleExpr);
    Assert.assertTrue(cache.containsKey(key2));

    Assert.assertNotEquals(cache.get(key1), cache.get(key2));
  }
}
