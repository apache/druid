package org.apache.druid.memory;

import java.nio.ByteBuffer;

public interface BufferHolder
{
  int position();
  int capacity();
  ByteBuffer get();
}
