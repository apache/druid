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

package org.apache.druid.query.rowsandcols;

import com.google.common.collect.Lists;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.rowsandcols.concrete.FrameRowsAndColumns;
import org.apache.druid.query.rowsandcols.concrete.FrameRowsAndColumnsTest;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * This test base exists to enable testing of RowsAndColumns objects.  When an implementation adds itself to this test
 * it will automatically be tested against every semantic interface that also participates in this test suite (should
 * be all of them).
 * <p>
 * These test suites are combined a bit precariously, so there is work that the developer needs to do to make sure
 * that things are wired up correctly.  Specifically, a developer must register their RowsAndColumns implementation
 * by adding an entry to the static {@link #makerFeeder()} method on this base class.  The developer should *also*
 * create a test class for their RowsAndColumns object that extends this class.  By creating the test class that
 * extends this class, there will be an extra validation done that ensures that the list of makers includes their
 * RowsAndColumns class.
 * <p>
 * The semantic interfaces, on the other hand, should all create a test that extends
 * {@link org.apache.druid.query.rowsandcols.semantic.SemanticTestBase}.  That test sets up a parameterized test,
 * using the results of {@link #makerFeeder()} to do the parameterization.
 */
public abstract class RowsAndColumnsTestBase
{
  static {
    NullHandling.initializeForTests();
  }

  private final Class<?> expectedClass;

  private static final AtomicReference<Iterable<Object[]>> MAKERS = new AtomicReference<>();

  @Nonnull
  private static ArrayList<Object[]> getMakers()
  {
    return Lists.newArrayList(
        new Object[]{MapOfColumnsRowsAndColumns.class, Function.identity()},
        new Object[]{ArrayListRowsAndColumns.class, ArrayListRowsAndColumnsTest.MAKER},
        new Object[]{ConcatRowsAndColumns.class, ConcatRowsAndColumnsTest.MAKER},
        new Object[]{RearrangedRowsAndColumns.class, RearrangedRowsAndColumnsTest.MAKER},
        new Object[]{FrameRowsAndColumns.class, FrameRowsAndColumnsTest.MAKER},
        new Object[]{StorageAdapterRowsAndColumns.class, StorageAdapterRowsAndColumnsTest.MAKER}
    );
  }

  public static Iterable<Object[]> makerFeeder()
  {
    Iterable<Object[]> retVal = MAKERS.get();
    if (retVal == null) {
      retVal = getMakers();
      for (Object[] objects : retVal) {
        Class<?> aClazz = (Class<?>) objects[0];
        final String expectedName = aClazz.getName() + "Test";
        try {
          final Class<?> testClass = Class.forName(expectedName);
          if (!RowsAndColumnsTestBase.class.isAssignableFrom(testClass)) {
            throw new ISE("testClass[%s] doesn't extend RowsAndColumnsTestBase, please extend it.", testClass);
          }
        }
        catch (ClassNotFoundException e) {
          throw new ISE("aClazz[%s] didn't have test class[%s], please make it", aClazz, expectedName);
        }
      }

      MAKERS.set(retVal);
    }
    return retVal;
  }

  public RowsAndColumnsTestBase(
      Class<?> expectedClass
  )
  {
    this.expectedClass = expectedClass;
  }

  @Test
  public void testInListOfMakers()
  {
    boolean inList = false;
    for (Object[] objs : makerFeeder()) {
      if (expectedClass.equals(objs[0])) {
        inList = true;
        break;
      }
    }
    Assert.assertTrue(inList);
  }
}
