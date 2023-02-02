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

package org.apache.druid.segment.virtual;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class ExpressionVectorSelectorsCastTest
{
  private static final int ROWS_PER_SEGMENT = 10_000;

  private static QueryableIndex INDEX;
  private static Closer CLOSER;

  @BeforeClass
  public static void setupClass()
  {
    CLOSER = Closer.create();

    final GeneratorSchemaInfo schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get("expression-testbench");

    final DataSegment dataSegment = DataSegment.builder()
                                               .dataSource("foo")
                                               .interval(schemaInfo.getDataInterval())
                                               .version("1")
                                               .shardSpec(new LinearShardSpec(0))
                                               .size(0)
                                               .build();

    final SegmentGenerator segmentGenerator = CLOSER.register(new SegmentGenerator());
    INDEX = CLOSER.register(
        segmentGenerator.generate(dataSegment, schemaInfo, Granularities.HOUR, ROWS_PER_SEGMENT)
    );
  }

  @AfterClass
  public static void teardownClass() throws IOException
  {
    CLOSER.close();
  }

  @Test
  public void testCastObjectSelectorToValueSelector()
  {
    testCast(INDEX, "string1", ColumnType.LONG, CLOSER);
    testCast(INDEX, "string2", ColumnType.DOUBLE, CLOSER);
    testCast(INDEX, "string3", ColumnType.FLOAT, CLOSER);
  }

  @Test
  public void testCastValueSelectorSelectorToObjectSelector()
  {
    testCast(INDEX, "long1", ColumnType.STRING, CLOSER);
    testCast(INDEX, "long2", ColumnType.STRING, CLOSER);
    testCast(INDEX, "double1", ColumnType.STRING, CLOSER);
    testCast(INDEX, "double2", ColumnType.STRING, CLOSER);
    testCast(INDEX, "float1", ColumnType.STRING, CLOSER);
    testCast(INDEX, "float2", ColumnType.STRING, CLOSER);
  }

  public static void testCast(
      QueryableIndex index,
      String column,
      ColumnType castTo,
      Closer closer
  )
  {
    final VirtualColumns virtualColumns = VirtualColumns.create(
        ImmutableList.of(
            new ExpressionVirtualColumn(
                "v",
                "cast(" + column + ", '" + ExpressionType.fromColumnType(castTo) + "')",
                castTo,
                TestExprMacroTable.INSTANCE
            )
        )
    );
    final QueryableIndexStorageAdapter storageAdapter = new QueryableIndexStorageAdapter(index);
    VectorCursor cursor = storageAdapter.makeVectorCursor(
        null,
        index.getDataInterval(),
        virtualColumns,
        false,
        512,
        null
    );
    closer.register(cursor);

    ColumnCapabilities capabilities = INDEX.getColumnCapabilities(column);

    if (capabilities.isNumeric() && castTo.is(ValueType.STRING)) {
      // numeric -> string
      verifyNumericToString(column, castTo, cursor);
    } else {
      // string -> numeric
      verifyStringToNumeric(column, castTo, cursor);
    }
  }

  private static void verifyStringToNumeric(String column, ColumnType castTo, VectorCursor cursor)
  {
    VectorValueSelector selector = cursor.getColumnSelectorFactory().makeValueSelector("v");
    VectorValueSelector castSelector = ExpressionVectorSelectors.castObjectSelectorToNumeric(
        cursor.getColumnSelectorFactory().getReadableVectorInspector(),
        column,
        cursor.getColumnSelectorFactory().makeObjectSelector(column),
        ColumnType.STRING,
        castTo
    );
    while (!cursor.isDone()) {
      boolean[] nulls;
      boolean[] castNulls;
      switch (castTo.getType()) {
        case LONG:
          nulls = selector.getNullVector();
          castNulls = castSelector.getNullVector();
          long[] longs = selector.getLongVector();
          long[] castLongs = castSelector.getLongVector();
          for (int i = 0; i < selector.getCurrentVectorSize(); i++) {
            if (nulls != null) {
              Assert.assertEquals(nulls[i], castNulls[i]);
            }
            Assert.assertEquals(longs[i], castLongs[i]);
          }
          break;
        case DOUBLE:
          nulls = selector.getNullVector();
          castNulls = selector.getNullVector();
          double[] doubles = selector.getDoubleVector();
          double[] castDoubles = castSelector.getDoubleVector();
          for (int i = 0; i < selector.getCurrentVectorSize(); i++) {
            if (nulls != null) {
              Assert.assertEquals(nulls[i], castNulls[i]);
            }
            Assert.assertEquals(doubles[i], castDoubles[i], 0.0);
          }
          break;

        case FLOAT:
          nulls = selector.getNullVector();
          castNulls = selector.getNullVector();
          float[] floats = selector.getFloatVector();
          float[] castFloats = castSelector.getFloatVector();
          for (int i = 0; i < selector.getCurrentVectorSize(); i++) {
            if (nulls != null) {
              Assert.assertEquals(nulls[i], castNulls[i]);
            }
            Assert.assertEquals(floats[i], castFloats[i], 0.0);
          }
          break;
        default:
          Assert.fail("this shouldn't happen");
          return;
      }

      cursor.advance();
    }
  }

  private static void verifyNumericToString(String column, ColumnType castTo, VectorCursor cursor)
  {
    VectorObjectSelector objectSelector = cursor.getColumnSelectorFactory().makeObjectSelector("v");
    VectorObjectSelector castSelector = ExpressionVectorSelectors.castValueSelectorToObject(
        cursor.getColumnSelectorFactory().getReadableVectorInspector(),
        column,
        cursor.getColumnSelectorFactory().makeValueSelector(column),
        cursor.getColumnSelectorFactory().getColumnCapabilities(column).toColumnType(),
        castTo
    );
    while (!cursor.isDone()) {
      switch (castTo.getType()) {
        case STRING:
          Object[] objects = objectSelector.getObjectVector();
          Object[] otherObjects = castSelector.getObjectVector();
          Assert.assertEquals(objectSelector.getCurrentVectorSize(), castSelector.getCurrentVectorSize());
          for (int i = 0; i < objectSelector.getCurrentVectorSize(); i++) {
            Assert.assertEquals(objects[i], otherObjects[i]);
          }
          break;
        default:
          Assert.fail("this shouldn't happen");
          return;
      }

      cursor.advance();
    }
  }
}
