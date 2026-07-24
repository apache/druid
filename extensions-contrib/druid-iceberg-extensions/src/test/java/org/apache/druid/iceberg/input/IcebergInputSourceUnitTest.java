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

package org.apache.druid.iceberg.input;

import org.apache.druid.java.util.common.UOE;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.StructLike;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class IcebergInputSourceUnitTest
{
  @Test
  public void testToDeleteFileInfoMapsPositionDeletes()
  {
    final DeleteFile pos = new StubDeleteFile(FileContent.POSITION_DELETES, "p.parquet", null);
    final DeleteFileInfo info = IcebergInputSource.toDeleteFileInfo(pos);
    Assert.assertEquals(DeleteFileInfo.ContentType.POSITION, info.getContentType());
    Assert.assertEquals("p.parquet", info.getPath());
    Assert.assertTrue(info.getEqualityFieldIds().isEmpty());
  }

  @Test
  public void testToDeleteFileInfoMapsEqualityDeletes()
  {
    final DeleteFile eq = new StubDeleteFile(FileContent.EQUALITY_DELETES, "e.parquet", Collections.singletonList(1));
    final DeleteFileInfo info = IcebergInputSource.toDeleteFileInfo(eq);
    Assert.assertEquals(DeleteFileInfo.ContentType.EQUALITY, info.getContentType());
    Assert.assertEquals("e.parquet", info.getPath());
    Assert.assertEquals(Collections.singletonList(1), info.getEqualityFieldIds());
  }

  @Test
  public void testToDeleteFileInfoRejectsUnsupportedContentType()
  {
    final DeleteFile unsupported = new StubDeleteFile(FileContent.DATA, "dv.puffin", null);
    final UOE thrown = Assert.assertThrows(
        UOE.class,
        () -> IcebergInputSource.toDeleteFileInfo(unsupported)
    );
    Assert.assertTrue(
        "Error message should call out deletion vectors",
        thrown.getMessage().contains("Deletion vectors")
    );
    Assert.assertTrue(
        "Error message should include the offending content type",
        thrown.getMessage().contains("DATA")
    );
  }

  private static final class StubDeleteFile implements DeleteFile
  {
    private final FileContent content;
    private final String path;
    private final List<Integer> equalityFieldIds;

    StubDeleteFile(final FileContent content, final String path, final List<Integer> equalityFieldIds)
    {
      this.content = content;
      this.path = path;
      this.equalityFieldIds = equalityFieldIds;
    }

    @Override
    public FileContent content()
    {
      return content;
    }

    @Override
    public CharSequence path()
    {
      return path;
    }

    @Override
    public List<Integer> equalityFieldIds()
    {
      return equalityFieldIds;
    }

    @Override
    public Long pos()
    {
      return null;
    }

    @Override
    public int specId()
    {
      return 0;
    }

    @Override
    public FileFormat format()
    {
      return FileFormat.PARQUET;
    }

    @Override
    public StructLike partition()
    {
      return null;
    }

    @Override
    public long recordCount()
    {
      return 0L;
    }

    @Override
    public long fileSizeInBytes()
    {
      return 0L;
    }

    @Override
    public Map<Integer, Long> columnSizes()
    {
      return Collections.emptyMap();
    }

    @Override
    public Map<Integer, Long> valueCounts()
    {
      return Collections.emptyMap();
    }

    @Override
    public Map<Integer, Long> nullValueCounts()
    {
      return Collections.emptyMap();
    }

    @Override
    public Map<Integer, Long> nanValueCounts()
    {
      return Collections.emptyMap();
    }

    @Override
    public Map<Integer, ByteBuffer> lowerBounds()
    {
      return Collections.emptyMap();
    }

    @Override
    public Map<Integer, ByteBuffer> upperBounds()
    {
      return Collections.emptyMap();
    }

    @Override
    public ByteBuffer keyMetadata()
    {
      return null;
    }

    @Override
    public List<Long> splitOffsets()
    {
      return null;
    }

    @Override
    public DeleteFile copy()
    {
      return this;
    }

    @Override
    public DeleteFile copyWithoutStats()
    {
      return this;
    }
  }
}
