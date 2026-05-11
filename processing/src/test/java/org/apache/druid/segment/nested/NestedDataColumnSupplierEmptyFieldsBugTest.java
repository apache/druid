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

package org.apache.druid.segment.nested;

import org.apache.druid.guice.BuiltInTypesModule;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.virtual.NestedFieldVirtualColumn;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class NestedDataColumnSupplierEmptyFieldsBugTest
{
  @BeforeAll
  public static void staticSetup()
  {
    BuiltInTypesModule.registerHandlersAndSerde();
  }

  @TempDir
  File tempDir;

  @Test
  public void testReadSegmentWithBuggyPaths() throws IOException
  {
    // segment data:
    // {"timestamp": "2026-01-01T00:00:00", "str":"b",     "obj":{"": {"a": 123}}}
    // {"timestamp": "2026-01-01T00:00:00", "str":"a",    "obj":{"": [{"a":456}]}}
    // prior to https://github.com/apache/druid/pull/19072 this would fail with an error like
    // org.apache.druid.error.DruidException: jq path [$[''].a] is invalid, path parts separated by '.' must not be empty
    // (which was also incorrect since it is a JSONPath not jq path)
    File tmpLocation = new File(tempDir, "druid.segment");
    Files.copy(
        NestedDataColumnSupplierEmptyFieldsBugTest.class.getClassLoader()
                                                        .getResourceAsStream("nested_segment_empty_fieldname_bug/druid.segment"),
        tmpLocation.toPath(),
        StandardCopyOption.REPLACE_EXISTING
    );
    try (Closer closer = Closer.create()) {
      QueryableIndex theIndex = closer.register(TestHelper.getTestIndexIO().loadIndex(tempDir));
      ColumnHolder columnHolder = theIndex.getColumnHolder("obj");
      Assertions.assertNotNull(columnHolder);
      NestedDataColumnV5<?, ?> v5 = closer.register((NestedDataColumnV5<?, ?>) columnHolder.getColumn());
      Assertions.assertNotNull(v5);
      NestedFieldVirtualColumn vc1 = new NestedFieldVirtualColumn(
          "obj",
          "$[''].a",
          "v0"
      );
      NestedFieldVirtualColumn vc2 = new NestedFieldVirtualColumn(
          "obj",
          "$[''][0].a",
          "v1"
      );
      VirtualColumns vc = VirtualColumns.create(vc1, vc2);
      QueryableIndexCursorFactory cursorFactory = new QueryableIndexCursorFactory(theIndex);
      CursorHolder cursorHolder = closer.register(cursorFactory.makeCursorHolder(CursorBuildSpec.builder().setVirtualColumns(vc).build()));

      VectorCursor cursor = cursorHolder.asVectorCursor();
      VectorObjectSelector v0Selector = cursor.getColumnSelectorFactory().makeObjectSelector("v0");
      VectorObjectSelector v1Selector = cursor.getColumnSelectorFactory().makeObjectSelector("v1");

      Object[] v0vals = v0Selector.getObjectVector();
      Object[] v1vals = v1Selector.getObjectVector();

      Assertions.assertEquals("123", v0vals[0]);
      Assertions.assertNull(v0vals[1]);

      Assertions.assertNull(v1vals[0]);
      Assertions.assertEquals("456", v1vals[1]);
    }
  }
}
