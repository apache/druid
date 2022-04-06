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

import com.fasterxml.jackson.databind.InjectableValues.Std;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.segment.IndexIO.V9IndexLoader;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class V9IndexLoaderTest extends InitializedNullHandlingTest
{
  private static final String COUNT_COLUMN = "count";

  @Test
  public void testLoadSegmentDamagedFileWithLazy() throws IOException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(new Std().addValue(ExprMacroTable.class, ExprMacroTable.nil()));
    final CannotDeserializeCountColumnV9IndexLoader loader = new CannotDeserializeCountColumnV9IndexLoader();
    String path = this.getClass().getClassLoader().getResource("v9SegmentPersistDir/segmentWithDamagedFile/").getPath();

    ForkSegmentLoadDropHandler segmentLoadDropHandler = new ForkSegmentLoadDropHandler();
    ForkSegment segment = new ForkSegment(true);
    Assert.assertTrue(segment.getSegmentExist());
    File inDir = new File(path);

    QueryableIndex queryableIndex = loader.load(
        inDir,
        mapper,
        true,
        () -> segmentLoadDropHandler.removeSegment(segment)
    );
    Assert.assertNotNull(queryableIndex);
    assertFailToDeserializeColumn(queryableIndex::getDimensionHandlers);
    List<String> columnNames = queryableIndex.getColumnNames();
    for (String columnName : columnNames) {
      if (COUNT_COLUMN.equals(columnName)) {
        assertFailToDeserializeColumn(() -> queryableIndex.getColumnHolder(columnName));
      } else {
        Assert.assertNotNull(queryableIndex.getColumnHolder(columnName));
      }
    }
    Assert.assertFalse(segment.getSegmentExist());
  }

  private static void assertFailToDeserializeColumn(Runnable runnable)
  {
    try {
      runnable.run();
    }
    catch (Exception e) {
      Assert.assertTrue(e instanceof RuntimeException);
      Assert.assertTrue(e.getCause() instanceof IOException);
      Assert.assertTrue(e.getMessage().contains("Exception test while deserializing a column"));
    }
  }

  private static class ForkSegmentLoadDropHandler
  {
    public void addSegment()
    {
    }

    public void removeSegment(ForkSegment segment)
    {
      segment.setSegmentExist(false);
    }
  }

  private static class ForkSegment
  {
    private Boolean segmentExist;

    ForkSegment(Boolean segmentExist)
    {
      this.segmentExist = segmentExist;
    }

    void setSegmentExist(Boolean value)
    {
      this.segmentExist = value;
    }

    Boolean getSegmentExist()
    {
      return this.segmentExist;
    }
  }

  private static class CannotDeserializeCountColumnV9IndexLoader extends V9IndexLoader
  {
    private CannotDeserializeCountColumnV9IndexLoader()
    {
      super(() -> 0);
    }

    @Override
    ColumnHolder deserializeColumn(
        String columnName,
        ObjectMapper mapper,
        ByteBuffer byteBuffer,
        SmooshedFileMapper smooshedFiles
    ) throws IOException
    {
      if (COUNT_COLUMN.equals(columnName)) {
        throw new IOException("Exception test while deserializing a column");
      }
      return super.deserializeColumn(
          columnName,
          mapper,
          byteBuffer,
          smooshedFiles
      );
    }
  }
}
