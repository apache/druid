package com.metamx.druid.kv;

/**
 */
public class Indexedids
{
  public static int[] arrayFromIndexedInts(IndexedInts ints)
  {
    int[] retVal = new int[ints.size()];
    for (int i = 0; i < ints.size(); ++i) {
      retVal[i] = ints.get(i);
    }
    return retVal;
  }
}
