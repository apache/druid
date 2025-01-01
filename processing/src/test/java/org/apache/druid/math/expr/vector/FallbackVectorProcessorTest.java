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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.Function;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FallbackVectorProcessorTest extends InitializedNullHandlingTest
{
  private static final int VECTOR_ID = 0;
  private static final int VECTOR_SIZE = 3;
  private static final RowSignature SIGNATURE =
      RowSignature.builder()
                  .add("long", ColumnType.LONG)
                  .add("double", ColumnType.DOUBLE)
                  .add("doubleNoNulls", ColumnType.DOUBLE)
                  .add("str", ColumnType.STRING)
                  .add("arr", ColumnType.LONG_ARRAY)
                  .build();

  private static final Map<String, List<Object>> DATA =
      ImmutableMap.<String, List<Object>>builder()
                  .put("long", Arrays.asList(101L, null, 103L))
                  .put("double", Arrays.asList(1.1, null, 1.3))
                  .put("doubleNoNulls", Arrays.asList(1.1, 1.2, 1.3))
                  .put("str", Arrays.asList("foo", null, "baz"))
                  .put(
                      "arr",
                      Arrays.asList(
                          new Object[]{201L, null, 203L},
                          null,
                          new Object[]{301L, null, 303L}
                      )
                  )
                  .build();

  private Expr.VectorInputBinding binding;

  @Before
  public void setUp()
  {
    binding = new Expr.VectorInputBinding()
    {
      @Override
      public Object[] getObjectVector(String name)
      {
        if (getType(name).isNumeric()) {
          throw new ISE("Incorrect call for field[%s] of type[%s]", name, getType(name));
        }

        return DATA.get(name).toArray();
      }

      @Override
      public long[] getLongVector(String name)
      {
        if (!getType(name).equals(ExpressionType.LONG)) {
          throw new ISE("Incorrect call for field[%s] of type[%s]", name, getType(name));
        }

        final List<Object> longs = DATA.get(name);
        final long[] longArray = new long[VECTOR_SIZE];

        for (int i = 0; i < longs.size(); i++) {
          Object o = longs.get(i);
          longArray[i] = o == null ? 0 : (long) o;
        }

        return longArray;
      }

      @Override
      public double[] getDoubleVector(String name)
      {
        if (!getType(name).equals(ExpressionType.DOUBLE)) {
          throw new ISE("Incorrect call for field[%s] of type[%s]", name, getType(name));
        }

        final List<Object> doubles = DATA.get(name);
        final double[] doubleArray = new double[VECTOR_SIZE];

        for (int i = 0; i < doubles.size(); i++) {
          Object o = doubles.get(i);
          doubleArray[i] = o == null ? 0 : (double) o;
        }

        return doubleArray;
      }

      @Nullable
      @Override
      public boolean[] getNullVector(String name)
      {
        final List<Object> objects = DATA.get(name);
        final boolean[] nullArray = new boolean[VECTOR_SIZE];

        boolean anyNulls = false;
        for (int i = 0; i < objects.size(); i++) {
          Object o = objects.get(i);
          nullArray[i] = o == null;
          anyNulls = anyNulls || o == null;
        }

        return anyNulls ? nullArray : null;
      }

      @Override
      public int getCurrentVectorSize()
      {
        return VECTOR_SIZE;
      }

      @Override
      public int getCurrentVectorId()
      {
        return VECTOR_ID;
      }

      @Override
      public int getMaxVectorSize()
      {
        return VECTOR_SIZE;
      }

      @Nullable
      @Override
      public ExpressionType getType(String name)
      {
        return SIGNATURE.getColumnType(name).map(ExpressionType::fromColumnType).orElse(null);
      }
    };
  }

  @Test
  public void test_case_long_double()
  {
    final FallbackVectorProcessor<Object[]> processor = FallbackVectorProcessor.create(
        new Function.CaseSimpleFunc(),
        ImmutableList.of(
            Parser.parse("long + double", ExprMacroTable.nil()),
            Parser.parse("102.1", ExprMacroTable.nil()),
            Parser.parse("long", ExprMacroTable.nil()),
            Parser.parse("double", ExprMacroTable.nil())
        ),
        binding
    );

    final ExprEvalVector<Object[]> eval = processor.evalVector(binding);

    Assert.assertEquals(ExpressionType.DOUBLE, eval.getType());
    Assert.assertArrayEquals(
        new Object[]{101.0, NullHandling.defaultDoubleValue(), 1.3},
        eval.getObjectVector()
    );
    Assert.assertArrayEquals(
        new double[]{101.0, 0L, 1.3},
        eval.getDoubleVector(),
        0
    );
    Assert.assertArrayEquals(
        NullHandling.sqlCompatible() ? new boolean[]{false, true, false} : null,
        eval.getNullVector()
    );
  }

  @Test
  public void test_upper_string()
  {
    final FallbackVectorProcessor<Object[]> processor = FallbackVectorProcessor.create(
        new Function.UpperFunc(),
        ImmutableList.of(
            Parser.parse("str", ExprMacroTable.nil())
        ),
        binding
    );

    final ExprEvalVector<Object[]> eval = processor.evalVector(binding);

    Assert.assertEquals(ExpressionType.STRING, eval.getType());
    Assert.assertArrayEquals(
        new Object[]{"FOO", null, "BAZ"},
        eval.getObjectVector()
    );
  }

  @Test
  public void test_concat_string_doubleNoNulls()
  {
    final FallbackVectorProcessor<Object[]> processor = FallbackVectorProcessor.create(
        new Function.ConcatFunc(),
        ImmutableList.of(
            Parser.parse("str", ExprMacroTable.nil()),
            Parser.parse("doubleNoNulls + 2", ExprMacroTable.nil())
        ),
        binding
    );

    final ExprEvalVector<Object[]> eval = processor.evalVector(binding);

    Assert.assertEquals(ExpressionType.STRING, eval.getType());
    Assert.assertArrayEquals(
        new Object[]{"foo3.1", NullHandling.sqlCompatible() ? null : "3.2", "baz3.3"},
        eval.getObjectVector()
    );
  }

  @Test
  public void test_array_length()
  {
    final FallbackVectorProcessor<Object[]> processor = FallbackVectorProcessor.create(
        new Function.ArrayLengthFunction(),
        ImmutableList.of(
            Parser.parse("arr", ExprMacroTable.nil())
        ),
        binding
    );

    final ExprEvalVector<Object[]> eval = processor.evalVector(binding);

    Assert.assertEquals(ExpressionType.LONG, eval.getType());
    Assert.assertArrayEquals(
        new Object[]{3L, null, 3L},
        eval.getObjectVector()
    );
    Assert.assertArrayEquals(
        new long[]{3L, 0L, 3L},
        eval.getLongVector()
    );
    Assert.assertArrayEquals(
        new boolean[]{false, true, false},
        eval.getNullVector()
    );
  }

  @Test
  public void test_array_concat()
  {
    final FallbackVectorProcessor<Object[]> processor = FallbackVectorProcessor.create(
        new Function.ArrayConcatFunction(),
        ImmutableList.of(
            Parser.parse("arr", ExprMacroTable.nil()),
            Parser.parse("long", ExprMacroTable.nil())
        ),
        binding
    );

    final ExprEvalVector<Object[]> eval = processor.evalVector(binding);

    Assert.assertEquals(ExpressionType.LONG_ARRAY, eval.getType());
    Assert.assertArrayEquals(
        new Object[]{201L, null, 203L, 101L},
        (Object[]) eval.getObjectVector()[0]
    );
    Assert.assertNull(eval.getObjectVector()[1]);
    Assert.assertArrayEquals(
        new Object[]{301L, null, 303L, 103L},
        (Object[]) eval.getObjectVector()[2]
    );
  }
}
