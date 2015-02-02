/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package io.druid.query.aggregation.hyperloglog;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

/**
 *
 */

@RunWith(Parameterized.class)
@Ignore // Don't need to run every time
public class HyperLogLogSerdeBenchmarkTest extends AbstractBenchmark
{
  private final HyperLogLogCollector collector;
  private final long NUM_HASHES;
  public HyperLogLogSerdeBenchmarkTest(final HyperLogLogCollector collector, Long num_hashes)
  {
    this.collector = collector;
    this.NUM_HASHES = num_hashes;
  }

  private static final HashFunction hashFunction = Hashing.murmur3_128();

  @Parameterized.Parameters
  public static Collection<Object[]> getParameters()
  {
    return ImmutableList.<Object[]>of(
        (Object[]) Arrays.asList(new priorByteBufferSerializer(), new Long(1 << 10)).toArray(),
        (Object[]) Arrays.asList(new newByteBufferSerializer(), new Long(1 << 10)).toArray(),
        (Object[]) Arrays.asList(new newByteBufferSerializerWithPuts(), new Long(1 << 10)).toArray(),
        (Object[]) Arrays.asList(new priorByteBufferSerializer(), new Long(1 << 8)).toArray(),
        (Object[]) Arrays.asList(new newByteBufferSerializer(), new Long(1 << 8)).toArray(),
        (Object[]) Arrays.asList(new newByteBufferSerializerWithPuts(), new Long(1 << 8)).toArray(),
        (Object[]) Arrays.asList(new priorByteBufferSerializer(), new Long(1 << 5)).toArray(),
        (Object[]) Arrays.asList(new newByteBufferSerializer(), new Long(1 << 5)).toArray(),
        (Object[]) Arrays.asList(new newByteBufferSerializerWithPuts(), new Long(1 << 5)).toArray(),
        (Object[]) Arrays.asList(new priorByteBufferSerializer(), new Long(1 << 2)).toArray(),
        (Object[]) Arrays.asList(new newByteBufferSerializer(), new Long(1 << 2)).toArray(),
    (Object[]) Arrays.asList(new newByteBufferSerializerWithPuts(), new Long(1 << 2)).toArray()
    );
  }

  private static final class priorByteBufferSerializer extends HLLCV1
  {
    @Override
    public ByteBuffer toByteBuffer()
    {
      final ByteBuffer myBuffer = getStorageBuffer();
      final int initialPosition = getInitPosition();
      short numNonZeroRegisters = getNumNonZeroRegisters();

      // store sparsely
      if (myBuffer.remaining() == getNumBytesForDenseStorage() && numNonZeroRegisters < DENSE_THRESHOLD) {
        ByteBuffer retVal = ByteBuffer.wrap(new byte[numNonZeroRegisters * 3 + getNumHeaderBytes()]);
        setVersion(retVal);
        setRegisterOffset(retVal, getRegisterOffset());
        setNumNonZeroRegisters(retVal, numNonZeroRegisters);
        setMaxOverflowValue(retVal, getMaxOverflowValue());
        setMaxOverflowRegister(retVal, getMaxOverflowRegister());

        int startPosition = getPayloadBytePosition();
        retVal.position(getPayloadBytePosition(retVal));
        for (int i = startPosition; i < startPosition + NUM_BYTES_FOR_BUCKETS; i++) {
          if (myBuffer.get(i) != 0) {
            retVal.putShort((short) (0xffff & (i - initialPosition)));
            retVal.put(myBuffer.get(i));
          }
        }
        retVal.rewind();
        return retVal.asReadOnlyBuffer();
      }

      return myBuffer.asReadOnlyBuffer();
    }
  }

  private static final class newByteBufferSerializer extends HLLCV1
  {
    @Override
    public ByteBuffer toByteBuffer()
    {

      final ByteBuffer myBuffer = getStorageBuffer();
      final int initialPosition = getInitPosition();
      final short numNonZeroRegisters = getNumNonZeroRegisters();

      // store sparsely
      if (myBuffer.remaining() == getNumBytesForDenseStorage() && numNonZeroRegisters < DENSE_THRESHOLD) {
        final ByteBuffer retVal = ByteBuffer.wrap(new byte[numNonZeroRegisters * 3 + getNumHeaderBytes()]);
        setVersion(retVal);
        setRegisterOffset(retVal, getRegisterOffset());
        setNumNonZeroRegisters(retVal, numNonZeroRegisters);
        setMaxOverflowValue(retVal, getMaxOverflowValue());
        setMaxOverflowRegister(retVal, getMaxOverflowRegister());

        final int startPosition = getPayloadBytePosition();
        retVal.position(getPayloadBytePosition(retVal));

        final byte[] zipperBuffer = new byte[NUM_BYTES_FOR_BUCKETS];
        ByteBuffer roStorageBuffer = myBuffer.asReadOnlyBuffer();
        roStorageBuffer.position(startPosition);
        roStorageBuffer.get(zipperBuffer);

        final ByteOrder byteOrder = retVal.order();

        final byte[] tempBuffer = new byte[numNonZeroRegisters * 3];
        int outBufferPos = 0;
        for (int i = 0; i < NUM_BYTES_FOR_BUCKETS; ++i) {
          if (zipperBuffer[i] != 0) {
            final short val = (short) (0xffff & (i + startPosition - initialPosition));
            if(byteOrder.equals(ByteOrder.LITTLE_ENDIAN)){
              tempBuffer[outBufferPos + 0] = (byte) (0xff & val);
              tempBuffer[outBufferPos + 1] = (byte) (0xff & (val>>8));
            }else{
              tempBuffer[outBufferPos + 1] = (byte) (0xff & val);
              tempBuffer[outBufferPos + 0] = (byte) (0xff & (val>>8));
            }
            tempBuffer[outBufferPos + 2] = zipperBuffer[i];
            outBufferPos += 3;
          }
        }
        retVal.put(tempBuffer);
        retVal.rewind();
        return retVal.asReadOnlyBuffer();
      }

      return myBuffer.asReadOnlyBuffer();
    }
  }


  private static final class newByteBufferSerializerWithPuts extends HLLCV1
  {
    @Override
    public ByteBuffer toByteBuffer()
    {
      final ByteBuffer myBuffer = getStorageBuffer();
      final int initialPosition = getInitPosition();

      final short numNonZeroRegisters = getNumNonZeroRegisters();

      // store sparsely
      if (myBuffer.remaining() == getNumBytesForDenseStorage() && numNonZeroRegisters < DENSE_THRESHOLD) {
        final ByteBuffer retVal = ByteBuffer.wrap(new byte[numNonZeroRegisters * 3 + getNumHeaderBytes()]);
        setVersion(retVal);
        setRegisterOffset(retVal, getRegisterOffset());
        setNumNonZeroRegisters(retVal, numNonZeroRegisters);
        setMaxOverflowValue(retVal, getMaxOverflowValue());
        setMaxOverflowRegister(retVal, getMaxOverflowRegister());

        final int startPosition = getPayloadBytePosition();
        retVal.position(getPayloadBytePosition(retVal));

        final byte[] zipperBuffer = new byte[NUM_BYTES_FOR_BUCKETS];
        ByteBuffer roStorageBuffer = myBuffer.asReadOnlyBuffer();
        roStorageBuffer.position(startPosition);
        roStorageBuffer.get(zipperBuffer);

        final ByteOrder byteOrder = retVal.order();

        for (int i = 0; i < NUM_BYTES_FOR_BUCKETS; ++i) {
          if (zipperBuffer[i] != 0) {
            final short val = (short) (0xffff & (i + startPosition - initialPosition));
            retVal.putShort(val);
            retVal.put(zipperBuffer[i]);
          }
        }
        retVal.rewind();
        return retVal.asReadOnlyBuffer();
      }

      return myBuffer.asReadOnlyBuffer();
    }
  }


  //--------------------------------------------------------------------------------------------------------------------



  private void fillCollector(HyperLogLogCollector collector)
  {
    Random rand = new Random(758190);
    for (long i = 0; i < NUM_HASHES; ++i) {
      collector.add(hashFunction.hashLong(rand.nextLong()).asBytes());
    }
  }

  private static HashCode getHash(final ByteBuffer byteBuffer)
  {
    Hasher hasher = hashFunction.newHasher();
    while (byteBuffer.position() < byteBuffer.limit()) {
      hasher.putByte(byteBuffer.get());
    }
    return hasher.hash();
  }

  @BeforeClass
  public static void setupHash()
  {

  }

  @Before
  public void setup()
  {
    fillCollector(collector);
  }


  volatile HashCode hashCode;

  @BenchmarkOptions(benchmarkRounds = 100000, warmupRounds = 100)
  @Test
  public void benchmarkToByteBuffer()
  {
    hashCode = getHash(collector.toByteBuffer());
  }
}
