package org.apache.druid.query.rowsandcols;

import java.util.List;

public interface GroupPartitioner
{
  /**
   * Computes the groupings of the underlying rows based on the columns passed in for grouping.  The grouping is
   * returned as an int[], the length of the array will be equal to the number of rows of data and the values of
   * the elements of the array will be the same when the rows are part of the same group and different when the
   * rows are part of different groups.
   *
   * @param columns the columns to group with
   * @return the groupings
   */
  int[] computeGroupings(List<String> columns);
}
