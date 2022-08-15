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

package org.apache.druid.segment.data;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.druid.java.util.common.io.Closer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

public class V3CompressedVSizeColumnarMultiIntsSupplierTest extends CompressedVSizeColumnarMultiIntsSupplierTestBase
{

  private Closer closer;
  private List<int[]> vals;
  private WritableSupplier<ColumnarMultiInts> columnarMultiIntsSupplier;

  @Override
  public void setUpSimple()
  {
    vals = Arrays.asList(
        new int[1],
        new int[]{1, 2, 3, 4, 5},
        new int[]{6, 7, 8, 9, 10},
        new int[]{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
    );
    closer = Closer.create();
    columnarMultiIntsSupplier = wrapSupplier(
        V3CompressedVSizeColumnarMultiIntsSupplier.fromIterable(
            Iterables.transform(vals, (Function<int[], ColumnarInts>) input -> VSizeColumnarInts.fromArray(input, 20)),
            2,
            20,
            ByteOrder.nativeOrder(),
            CompressionStrategy.LZ4,
            closer
        ),
        closer
    );
  }

  @Override
  public void teardown() throws IOException
  {
    columnarMultiIntsSupplier = null;
    closer.close();
    vals = null;
  }

  @Override
  public List<int[]> getValsUsed()
  {
    return vals;
  }

  @Override
  public WritableSupplier<ColumnarMultiInts> getColumnarMultiIntsSupplier()
  {
    return columnarMultiIntsSupplier;
  }

  @Override
  public WritableSupplier<ColumnarMultiInts> fromByteBuffer(ByteBuffer buffer)
  {
    return wrapSupplier(
        V3CompressedVSizeColumnarMultiIntsSupplier.fromByteBuffer(
            buffer,
            ByteOrder.nativeOrder()
        ),
        closer
    );
  }
}
