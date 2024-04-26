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

package org.apache.druid.frame.write;

import com.google.common.collect.ImmutableList;
import org.apache.druid.frame.allocation.AppendableMemory;
import org.apache.druid.frame.allocation.HeapMemoryAllocator;
import org.apache.druid.frame.field.LongFieldWriter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class RowBasedFrameWriterTest
{
  @Test
  public void testAddSelectionWithException()
  {
    String colName = "colName";
    String errorMsg = "Frame writer exception";

    final RowSignature signature = RowSignature.builder().add(colName, ColumnType.LONG).build();

    LongFieldWriter fieldWriter = EasyMock.mock(LongFieldWriter.class);
    EasyMock.expect(fieldWriter.writeTo(
        EasyMock.anyObject(),
        EasyMock.anyLong(),
        EasyMock.anyLong()
    )).andThrow(new RuntimeException(errorMsg));

    EasyMock.replay(fieldWriter);

    RowBasedFrameWriter rowBasedFrameWriter = new RowBasedFrameWriter(
        signature,
        Collections.emptyList(),
        ImmutableList.of(fieldWriter),
        null,
        null,
        AppendableMemory.create(HeapMemoryAllocator.unlimited()),
        AppendableMemory.create(HeapMemoryAllocator.unlimited())
    );

    InvalidFieldException expectedException = new InvalidFieldException.Builder()
        .column(colName)
        .errorMsg(errorMsg)
        .build();

    InvalidFieldException actualException = Assert.assertThrows(
        InvalidFieldException.class,
        rowBasedFrameWriter::addSelection
    );
    Assert.assertEquals(expectedException, actualException);
  }
}
