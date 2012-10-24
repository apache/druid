package com.metamx.druid.index.v1.processing;

import com.metamx.druid.kv.IndexedInts;

/**
 */
public interface DimensionSelector
{
  /**
   * Gets all values for the row inside of an IntBuffer.  I.e. one possible implementation could be
   *
   * return IntBuffer.wrap(lookupExpansion(get());
   *
   * @return all values for the row as an IntBuffer
   */
  public IndexedInts getRow();

  /**
   * Value cardinality is the cardinality of the different occurring values.  If there were 4 rows:
   *
   * A,B
   * A
   * B
   * A
   *
   * Value cardinality would be 2.
   *
   * @return
   */
  public int getValueCardinality();

  /**
   * The Name is the String name of the actual field.  It is assumed that storage layers convert names
   * into id values which can then be used to get the string value.  For example
   *
   * A,B
   * A
   * A,B
   * B
   *
   * would be turned into (per lookupExpansion)
   * 
   * 0
   * 1
   * 0
   * 2
   *
   * at which point lookupExpansion would really return:
   *
   * lookupExpansion(1) => [0 1]
   * lookupExpansion(2) => [0]
   * lookupExpansion(3) => [1]
   *
   * and then lookupName would return:
   *
   * lookupName(0) => A
   * lookupName(1) => B
   *
   * @param id
   * @return
   */
  public String lookupName(int id);

  /**
   * The ID is the int id value of the field.
   *    
   * @param name
   * @return
   */
  public int lookupId(String name);
}
