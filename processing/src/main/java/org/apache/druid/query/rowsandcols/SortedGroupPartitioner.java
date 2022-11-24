package org.apache.druid.query.rowsandcols;

import java.util.ArrayList;
import java.util.List;

/**
 * A semantic interface used to partition a data set based on a given set of dimensions.
 *
 * This specifically assumes that it is working with sorted data and, as such, the groups returned
 * should be contiguous and unique (that is, all rows for a given combination of values exist in only one grouping)
 */
public interface SortedGroupPartitioner
{
  /**
   * Computes and returns a list of contiguous boundaries for independent groups.  All rows in a specific grouping
   * should have the same values for the identified columns.  Additionally, as this is assuming it is dealing with
   * sorted data, there should only be a single entry in the return value for a given set of values of the columns.
   *
   * @param columns the columns to partition on
   * @return a list of start (inclusive) and end (exclusive) rowIds where each chunk of contiguous rows represents
   * a single grouping
   */
  ArrayList<StartAndEnd> computeBoundaries(List<String> columns);

  /**
   * Semantically equivalent to computeBoundaries, but returns a list of RowsAndColumns objects instead of just
   * boundary positions.  This is useful as it allows the concrete implementation to return RowsAndColumns objects
   * that are aware of the internal representation of the data and thus can provide optimized implementations of
   * other semantic interfaces as the "child" RowsAndColumns are used
   *
   * @param partitionColumns the columns to partition on
   * @return a list of RowsAndColumns representing the data grouped by the partition columns.
   */
  ArrayList<RowsAndColumns> partitionOnBoundaries(List<String> partitionColumns);
}
