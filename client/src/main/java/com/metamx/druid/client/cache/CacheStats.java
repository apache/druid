package com.metamx.druid.client.cache;

/**
 */
public class CacheStats
{
  private final long numHits;
  private final long numMisses;
  private final long size;
  private final long sizeInBytes;
  private final long numEvictions;

  public CacheStats(
      long numHits,
      long numMisses,
      long size,
      long sizeInBytes,
      long numEvictions
  )
  {
    this.numHits = numHits;
    this.numMisses = numMisses;
    this.size = size;
    this.sizeInBytes = sizeInBytes;
    this.numEvictions = numEvictions;
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
        numEvictions - oldStats.numEvictions
    );
  }
}
