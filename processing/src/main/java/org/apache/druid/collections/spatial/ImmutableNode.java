package org.apache.druid.collections.spatial;

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;

import java.nio.ByteBuffer;

public interface ImmutableNode<TCoordinatesArray>
{
  BitmapFactory getBitmapFactory();

  int getInitialOffset();

  int getOffsetFromInitial();

  int getNumDims();

  boolean isLeaf();

  TCoordinatesArray getMinCoordinates();

  TCoordinatesArray getMaxCoordinates();

  ImmutableBitmap getImmutableBitmap();

  @SuppressWarnings("ArgumentParameterSwap")
  Iterable<ImmutableNode<TCoordinatesArray>> getChildren();

  ByteBuffer getData();
}
