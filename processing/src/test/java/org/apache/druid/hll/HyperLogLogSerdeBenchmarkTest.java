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

package org.apache.druid.hll;

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
 * TODO rewrite to use JMH and move to the benchmarks project
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

  private static final HashFunction HASH_FUNCTION = Hashing.murmur3_128();

  @Parameterized.Parameters
  public static Collection<Object[]> getParameters()
  {
    return ImmutableList.of(
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

  private static final class priorByteBufferSerializer extends VersionOneHyperLogLogCollector
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

  private static final class newByteBufferSerializer extends VersionOneHyperLogLogCollector
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
            if (byteOrder.equals(ByteOrder.LITTLE_ENDIAN)) {
              tempBuffer[outBufferPos + 0] = (byte) (0xff & val);
              tempBuffer[outBufferPos + 1] = (byte) (0xff & (val >> 8));
            } else {
              tempBuffer[outBufferPos + 1] = (byte) (0xff & val);
              tempBuffer[outBufferPos + 0] = (byte) (0xff & (val >> 8));
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


  private static final class newByteBufferSerializerWithPuts extends VersionOneHyperLogLogCollector
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
      collector.add(HASH_FUNCTION.hashLong(rand.nextLong()).asBytes());
    }
  }

  private static HashCode getHash(final ByteBuffer byteBuffer)
  {
    Hasher hasher = HASH_FUNCTION.newHasher();
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


  @SuppressWarnings("unused")
  volatile HashCode hashCode;

  @BenchmarkOptions(benchmarkRounds = 100000, warmupRounds = 100)
  @Test
  public void benchmarkToByteBuffer()
  {
    hashCode = getHash(collector.toByteBuffer());
  }
}
