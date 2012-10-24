package com.metamx.druid.index.v1;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.metamx.druid.collect.ResourceHolder;
import com.metamx.druid.collect.StupidResourceHolder;
import com.metamx.druid.kv.GenericIndexed;
import com.metamx.druid.kv.IndexedLongs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.List;

/**
 */
public class InMemoryCompressedLongs implements IndexedLongs
{
  private final CompressedLongBufferObjectStrategy strategy;
  private final int sizePer;

  private List<byte[]> compressedBuffers = Lists.newArrayList();
  private int numInserted = 0;
  private int numCompressed = 0;

  private ResourceHolder<LongBuffer> holder = null;
  private LongBuffer loadBuffer = null;
  private int loadBufferIndex = -1;

  private LongBuffer endBuffer;

  public InMemoryCompressedLongs(
      int sizePer,
      ByteOrder order
  )
  {
    this.sizePer = sizePer;
    strategy = CompressedLongBufferObjectStrategy.getBufferForOrder(order);

    endBuffer = LongBuffer.allocate(sizePer);
    endBuffer.mark();
  }

  @Override
  public int size()
  {
    return numInserted;
  }

  public int add(long value)
  {
    if (! endBuffer.hasRemaining()) {
      endBuffer.rewind();
      compressedBuffers.add(strategy.toBytes(StupidResourceHolder.create(endBuffer)));
      endBuffer = LongBuffer.allocate(sizePer);
      endBuffer.mark();
      numCompressed += sizePer;
    }

    int retVal = numCompressed + endBuffer.position();
    endBuffer.put(value);

    ++numInserted;
    return retVal;
  }

  public int addAll(Iterable<Long> values)
  {
    int retVal = -1;
    for (Long value : values) {
      retVal = add(value);
    }
    return retVal;
  }

  @Override
  public long get(int index)
  {
    int bufferNum = index / sizePer;
    int bufferIndex = index % sizePer;

    if (bufferNum == compressedBuffers.size()) {
      return endBuffer.get(bufferIndex);
    }
    if (bufferNum != loadBufferIndex) {
      loadBuffer(bufferNum);
    }

    return loadBuffer.get(loadBuffer.position() + bufferIndex);
  }

  @Override
  public void fill(int index, long[] toFill)
  {
    if (size() - index < toFill.length) {
      throw new IndexOutOfBoundsException(
          String.format(
              "Cannot fill array of size[%,d] at index[%,d].  Max size[%,d]", toFill.length, index, size()
          )
      );
    }

    int bufferNum = index / sizePer;
    int bufferIndex = index % sizePer;

    int leftToFill = toFill.length;
    while (leftToFill > 0) {
      if (bufferNum == compressedBuffers.size()) {
        endBuffer.mark();
        endBuffer.position(bufferIndex);
        endBuffer.get(toFill, toFill.length - leftToFill, leftToFill);
        endBuffer.rewind();
        return;
      }
      if (bufferNum != loadBufferIndex) {
        loadBuffer(bufferNum);
      }

      loadBuffer.mark();
      loadBuffer.position(loadBuffer.position() + bufferIndex);
      final int numToGet = Math.min(loadBuffer.remaining(), leftToFill);
      loadBuffer.get(toFill, toFill.length - leftToFill, numToGet);
      loadBuffer.rewind();
      leftToFill -= numToGet;
      ++bufferNum;
      bufferIndex = 0;
    }
  }

  @Override
  public int binarySearch(long key)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int binarySearch(long key, int from, int to)
  {
    throw new UnsupportedOperationException();
  }

  private void loadBuffer(int bufferNum)
  {
    loadBuffer = null;
    Closeables.closeQuietly(holder);
    final byte[] compressedBytes = compressedBuffers.get(bufferNum);
    holder = strategy.fromByteBuffer(ByteBuffer.wrap(compressedBytes), compressedBytes.length);
    loadBuffer = holder.get();
    loadBufferIndex = bufferNum;
  }

  public CompressedLongsIndexedSupplier toCompressedLongsIndexedSupplier()
  {
    final LongBuffer longBufCopy = endBuffer.asReadOnlyBuffer();
    longBufCopy.flip();
    
    return new CompressedLongsIndexedSupplier(
        numInserted,
        sizePer,
        GenericIndexed.fromIterable(
            Iterables.<ResourceHolder<LongBuffer>>concat(
                Iterables.transform(
                    compressedBuffers,
                    new Function<byte[], ResourceHolder<LongBuffer>>()
                    {
                      @Override
                      public ResourceHolder<LongBuffer> apply(byte[] input)
                      {
                        return strategy.fromByteBuffer(ByteBuffer.wrap(input), input.length);
                      }
                    }
                ),
                Arrays.<ResourceHolder<LongBuffer>>asList(StupidResourceHolder.create(longBufCopy))
            ),
            strategy
        )
    );
  }

  @Override
  public void close() throws IOException
  {
    Closeables.close(holder, false);
  }
}
