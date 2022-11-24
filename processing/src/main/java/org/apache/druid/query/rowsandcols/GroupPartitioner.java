package org.apache.druid.query.rowsandcols;

import java.util.List;

/**
 * A semantic interface used to partition a data set based on a given set of dimensions.
 */
public interface GroupPartitioner
{
  /**
   * Computes the groupings of the underlying rows based on the columns passed in for grouping.  The grouping is
   * returned as an int[], the length of the array will be equal to the number of rows of data and the values of
   * the elements of the array will be the same when the rows are part of the same group and different when the
   * rows are part of different groups.  This is contrasted with the SortedGroupPartitioner in that, the
   * groupings returned are not necessarily contiguous.  There is also no sort-order implied by the `int` values
   * assigned to each grouping.
   *
   * @param columns the columns to group with
   * @return the groupings, rows with the same int value are in the same group.  There is no sort-order implied by the
   * int values.
   */
  int[] computeGroupings(List<String> columns);
}
