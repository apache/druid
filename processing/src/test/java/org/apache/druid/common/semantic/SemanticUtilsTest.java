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

package org.apache.druid.common.semantic;

import org.apache.druid.error.DruidException;
import org.apache.druid.segment.CloseableShapeshifter;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.function.Function;

public class SemanticUtilsTest
{
  @Test
  public void testInvalidParameters()
  {
    Assert.assertThrows(
        DruidException.class,
        () -> SemanticUtils.makeAsMap(InvalidShapeshifter.class)
    );
  }

  @Test
  public void testValidParameters()
  {
    TestShapeshifter testShapeshifter = new TestShapeshifter();
    Assert.assertTrue(testShapeshifter.as(A.class) instanceof A);
  }

  @Test
  public void testOverrideForNewMapping()
  {
    SemanticUtils.registerAsOverride(
        TestShapeshifter.class,
        OverrideClass.class,
        (testShapeshifter) -> new OverrideClass()
    );
    TestShapeshifter testShapeshifter = new TestShapeshifter();
    Assert.assertTrue(testShapeshifter.as(A.class) instanceof A);
    Assert.assertTrue(testShapeshifter.as(OverrideClass.class) instanceof OverrideClass);
  }

  @Test
  public void testOverrideForExistingMapping()
  {
    SemanticUtils.registerAsOverride(
        TestShapeshifter.class,
        A.class,
        (testShapeshifter) -> new OverrideClass()
    );
    TestShapeshifter testShapeshifter = new TestShapeshifter();
    Assert.assertTrue(testShapeshifter.as(A.class) instanceof OverrideClass);
  }

  static class TestShapeshifter implements CloseableShapeshifter
  {
    private final Map<Class<?>, Function<TestShapeshifter, ?>> asMap;

    public TestShapeshifter()
    {
      this.asMap = SemanticUtils.makeAsMap(TestShapeshifter.class);
    }

    @SuppressWarnings("unchecked")
    @Override
    @Nullable
    public <T> T as(@Nonnull Class<T> clazz)
    {
      //noinspection ReturnOfNull
      return (T) asMap.getOrDefault(clazz, arg -> null).apply(this);
    }

    @Override
    public void close()
    {
    }

    @SemanticCreator
    public AInterface toAInterface()
    {
      return new A();
    }
  }

  static class InvalidShapeshifter implements CloseableShapeshifter
  {
    @Nullable
    @Override
    public <T> T as(@Nonnull Class<T> clazz)
    {
      return null;
    }

    @Override
    public void close()
    {
    }

    @SemanticCreator
    public AInterface toAInterface(String invalidParameter)
    {
      return new A();
    }
  }

  interface AInterface
  {
  }

  static class A implements AInterface
  {
  }

  static class OverrideClass extends A
  {
  }
}
