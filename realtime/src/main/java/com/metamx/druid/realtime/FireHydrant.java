package com.metamx.druid.realtime;

import com.metamx.druid.StorageAdapter;
import com.metamx.druid.index.v1.IncrementalIndex;
import com.metamx.druid.index.v1.IncrementalIndexStorageAdapter;

/**
*/
public class FireHydrant
{
  private volatile IncrementalIndex index;
  private volatile StorageAdapter adapter;
  private final int count;

  public FireHydrant(
      IncrementalIndex index,
      int count
  )
  {
    this.index = index;
    this.adapter = new IncrementalIndexStorageAdapter(index);
    this.count = count;
  }

  public FireHydrant(
      StorageAdapter adapter,
      int count
  )
  {
    this.index = null;
    this.adapter = adapter;
    this.count = count;
  }

  public IncrementalIndex getIndex()
  {
    return index;
  }

  public StorageAdapter getAdapter()
  {
    return adapter;
  }

  public int getCount()
  {
    return count;
  }

  public boolean hasSwapped()
  {
    return index == null;
  }

  public void swapAdapter(StorageAdapter adapter)
  {
    this.adapter = adapter;
    this.index = null;
  }

  @Override
  public String toString()
  {
    return "FireHydrant{" +
           "index=" + index +
           ", queryable=" + adapter +
           ", count=" + count +
           '}';
  }
}
