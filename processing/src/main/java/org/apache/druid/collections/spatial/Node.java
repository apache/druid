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

package org.apache.druid.collections.spatial;

import com.google.common.base.Preconditions;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.MutableBitmap;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 */
public class Node
{
  private final float[] minCoordinates;
  private final float[] maxCoordinates;

  private final List<Node> children;
  private final boolean isLeaf;
  private final MutableBitmap bitmap;

  private Node parent;

  public Node(float[] minCoordinates, float[] maxCoordinates, boolean isLeaf, BitmapFactory bitmapFactory)
  {
    this(
        minCoordinates,
        maxCoordinates,
        null,
        isLeaf,
        null,
        bitmapFactory.makeEmptyMutableBitmap()
    );
  }

  /**
   * This constructor accepts a single nullable child Node (null value means no child) instead of a collection of
   * children Nodes, because Nodes with no more than one child are created in the codebase yet, while passing a
   * collection of Nodes would necessitate making a defensive copy of this collection in the constructor and extra
   * overhead.
   *
   * (One could note that the principle of making a defensive copy is happily violated just in this
   * constructor, other parameters: minCoordinates, maxCoordinates and bitmap. These are recognized flaws that are not
   * tackled yet.)
   *
   * If cases when a Node should be created with multiple children arise, this constructor should be changed to accept
   * a collection of children Nodes.
   */
  public Node(
      float[] minCoordinates,
      float[] maxCoordinates,
      @Nullable Node child,
      boolean isLeaf,
      Node parent,
      MutableBitmap bitmap
  )
  {
    Preconditions.checkArgument(minCoordinates.length == maxCoordinates.length);

    this.minCoordinates = minCoordinates;
    this.maxCoordinates = maxCoordinates;
    this.children = new ArrayList<>(1);
    if (child != null) {
      children.add(child);
      child.setParent(this);
    }
    this.isLeaf = isLeaf;
    this.bitmap = bitmap;
    this.parent = parent;
  }

  public int getNumDims()
  {
    return minCoordinates.length;
  }

  public float[] getMinCoordinates()
  {
    return minCoordinates;
  }

  public float[] getMaxCoordinates()
  {
    return maxCoordinates;
  }

  public Node getParent()
  {
    return parent;
  }

  private void setParent(Node p)
  {
    parent = p;
  }

  public void addChild(Node node)
  {
    node.setParent(this);
    children.add(node);
  }

  public List<Node> getChildren()
  {
    return children;
  }

  public boolean isLeaf()
  {
    return isLeaf;
  }

  public double getArea()
  {
    return calculateArea();
  }

  public boolean contains(float[] coords)
  {
    Preconditions.checkArgument(getNumDims() == coords.length);

    for (int i = 0; i < getNumDims(); i++) {
      if (coords[i] < minCoordinates[i] || coords[i] > maxCoordinates[i]) {
        return false;
      }
    }
    return true;
  }

  public boolean enclose()
  {
    boolean retVal = false;
    float[] minCoords = new float[getNumDims()];
    Arrays.fill(minCoords, Float.POSITIVE_INFINITY);
    float[] maxCoords = new float[getNumDims()];
    Arrays.fill(maxCoords, Float.NEGATIVE_INFINITY);

    for (Node child : getChildren()) {
      for (int i = 0; i < getNumDims(); i++) {
        minCoords[i] = Math.min(child.getMinCoordinates()[i], minCoords[i]);
        maxCoords[i] = Math.max(child.getMaxCoordinates()[i], maxCoords[i]);
      }
    }

    if (!Arrays.equals(minCoords, minCoordinates)) {
      System.arraycopy(minCoords, 0, minCoordinates, 0, minCoordinates.length);
      retVal = true;
    }
    if (!Arrays.equals(maxCoords, maxCoordinates)) {
      System.arraycopy(maxCoords, 0, maxCoordinates, 0, maxCoordinates.length);
      retVal = true;
    }

    return retVal;
  }

  public MutableBitmap getBitmap()
  {
    return bitmap;
  }

  public void addToBitmapIndex(Node node)
  {
    bitmap.or(node.getBitmap());
  }

  public void clear()
  {
    children.clear();
    bitmap.clear();
  }

  public int getSizeInBytes()
  {
    return ImmutableNode.HEADER_NUM_BYTES
           + 2 * getNumDims() * Float.BYTES
           + Integer.BYTES // size of the set
           + bitmap.getSizeInBytes()
           + getChildren().size() * Integer.BYTES;
  }

  public int storeInByteBuffer(ByteBuffer buffer, int position)
  {
    buffer.position(position);
    buffer.putShort((short) (((isLeaf ? 0x1 : 0x0) << 15) | getChildren().size()));
    for (float v : getMinCoordinates()) {
      buffer.putFloat(v);
    }
    for (float v : getMaxCoordinates()) {
      buffer.putFloat(v);
    }
    byte[] bytes = bitmap.toBytes();
    buffer.putInt(bytes.length);
    buffer.put(bytes);

    int pos = buffer.position();
    int childStartOffset = pos + getChildren().size() * Integer.BYTES;
    for (Node child : getChildren()) {
      buffer.putInt(pos, childStartOffset);
      childStartOffset = child.storeInByteBuffer(buffer, childStartOffset);
      pos += Integer.BYTES;
    }

    return childStartOffset;
  }

  private double calculateArea()
  {
    double area = 1.0;
    for (int i = 0; i < minCoordinates.length; i++) {
      area *= (maxCoordinates[i] - minCoordinates[i]);
    }
    return area;
  }
}
