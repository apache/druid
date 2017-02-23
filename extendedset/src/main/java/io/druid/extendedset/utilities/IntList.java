package io.druid.extendedset.utilities;

import java.nio.IntBuffer;
import java.util.ArrayList;

/**
 */
public class IntList
{
  private static final int ALLOCATION_SIZE = 1024;

  private final ArrayList<int[]> baseLists = new ArrayList<>();

  private int maxIndex = -1;

  public int length()
  {
    return maxIndex + 1;
  }

  public boolean isEmpty()
  {
    return (length() == 0);
  }

  public void add(int value)
  {
    set(length(), value);
  }

  public void set(int index, int value)
  {
    int subListIndex = index / ALLOCATION_SIZE;

    if (subListIndex >= baseLists.size()) {
      for (int i = baseLists.size(); i <= subListIndex; ++i) {
        baseLists.add(null);
      }
    }

    int[] baseList = baseLists.get(subListIndex);

    if (baseList == null) {
      baseList = new int[ALLOCATION_SIZE];
      baseLists.set(subListIndex, baseList);
    }

    baseList[index % ALLOCATION_SIZE] = value;

    if (index > maxIndex) {
      maxIndex = index;
    }
  }

  public int get(int index)
  {
    if (index > maxIndex) {
      throw new ArrayIndexOutOfBoundsException(index);
    }

    int subListIndex = index / ALLOCATION_SIZE;
    int[] baseList = baseLists.get(subListIndex);

    if (baseList == null) {
      return 0;
    }

    return baseList[index % ALLOCATION_SIZE];
  }

  public int baseListCount()
  {
    return baseLists.size();
  }

  public IntBuffer getBaseList(int index)
  {
    final int[] array = baseLists.get(index);
    if (array == null) {
      return null;
    }

    final IntBuffer retVal = IntBuffer.wrap(array);

    if (index + 1 == baseListCount()) {
      retVal.limit(maxIndex - (index * ALLOCATION_SIZE));
    }

    return retVal.asReadOnlyBuffer();
  }

  public int[] toArray()
  {
    int[] retVal = new int[length()];
    int currIndex = 0;
    for (int[] arr : baseLists) {
      int min = Math.min(length() - currIndex, arr.length);
      System.arraycopy(arr, 0, retVal, currIndex, min);
      currIndex += min;
    }
    return retVal;
  }
}
