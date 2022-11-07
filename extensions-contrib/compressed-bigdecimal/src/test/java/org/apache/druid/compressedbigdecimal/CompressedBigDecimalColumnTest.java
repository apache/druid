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

package org.apache.druid.compressedbigdecimal;

import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.ColumnarMultiInts;
import org.apache.druid.segment.data.ReadableOffset;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

public class CompressedBigDecimalColumnTest
{
  @Test
  public void testCompressedBigDecimalColumn()
  {
    ColumnarMultiInts columnarMultiInts = EasyMock.createMock(ColumnarMultiInts.class);
    ColumnarInts columnarInts = EasyMock.createMock(ColumnarInts.class);
    ReadableOffset readableOffset = EasyMock.createMock(ReadableOffset.class);
    CompressedBigDecimalColumn compressedBigDecimalColumn = new CompressedBigDecimalColumn(
        columnarInts,
        columnarMultiInts
    );
    Assert.assertEquals(
        CompressedBigDecimalModule.COMPRESSED_BIG_DECIMAL,
        compressedBigDecimalColumn.getTypeName()
    );
    Assert.assertEquals(0, compressedBigDecimalColumn.getLength());
    Assert.assertEquals(CompressedBigDecimalColumn.class, compressedBigDecimalColumn.getClazz());
    Assert.assertNotNull(compressedBigDecimalColumn.makeColumnValueSelector(readableOffset));
  }

}
