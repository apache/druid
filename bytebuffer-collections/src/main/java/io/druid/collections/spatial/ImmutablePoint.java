/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.collections.spatial;

import io.druid.collections.bitmap.BitmapFactory;

import java.nio.ByteBuffer;

public class ImmutablePoint extends ImmutableNode
{
  public ImmutablePoint(
      int numDims,
      int initialOffset,
      int offsetFromInitial,
      ByteBuffer data,
      BitmapFactory bitmapFactory
  )
  {
    super(numDims, initialOffset, offsetFromInitial, (short) 0, true, data, bitmapFactory);
  }

  public ImmutablePoint(ImmutableNode node)
  {
    super(
        node.getNumDims(),
        node.getInitialOffset(),
        node.getOffsetFromInitial(),
        (short) 0,
        true,
        node.getData(),
        node.getBitmapFactory()
    );
  }

  public float[] getCoords()
  {
    return super.getMinCoordinates();
  }

  @Override
  public Iterable<ImmutableNode> getChildren()
  {
    // should never get here
    throw new UnsupportedOperationException();
  }

}
