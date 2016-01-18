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

package io.druid.client.cache;

/**
 */
public class CacheStats
{
  private final long numHits;
  private final long numMisses;
  private final long size;
  private final long sizeInBytes;
  private final long numEvictions;
  private final long numTimeouts;
  private final long numErrors;

  public CacheStats(
      long numHits,
      long numMisses,
      long size,
      long sizeInBytes,
      long numEvictions,
      long numTimeouts,
      long numErrors
  )
  {
    this.numHits = numHits;
    this.numMisses = numMisses;
    this.size = size;
    this.sizeInBytes = sizeInBytes;
    this.numEvictions = numEvictions;
    this.numTimeouts = numTimeouts;
    this.numErrors = numErrors;
  }

  public long getNumHits()
  {
    return numHits;
  }

  public long getNumMisses()
  {
    return numMisses;
  }

  public long getNumEntries()
  {
    return size;
  }

  public long getSizeInBytes()
  {
    return sizeInBytes;
  }

  public long getNumEvictions()
  {
    return numEvictions;
  }

  public long getNumTimeouts()
  {
    return numTimeouts;
  }

  public long getNumErrors()
  {
    return numErrors;
  }

  public long numLookups()
  {
    return numHits + numMisses;
  }

  public double hitRate()
  {
    long lookups = numLookups();
    return lookups == 0 ? 0 : numHits / (double) lookups;
  }

  public long averageBytes()
  {
    return size == 0 ? 0 : sizeInBytes / size;
  }

  public CacheStats delta(CacheStats oldStats)
  {
    if (oldStats == null) {
      return this;
    }
    return new CacheStats(
        numHits - oldStats.numHits,
        numMisses - oldStats.numMisses,
        size - oldStats.size,
        sizeInBytes - oldStats.sizeInBytes,
        numEvictions - oldStats.numEvictions,
        numTimeouts - oldStats.numTimeouts,
        numErrors - oldStats.numErrors
    );
  }
}
