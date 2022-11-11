package org.apache.druid.query.rowsandcols;

import java.util.ArrayList;
import java.util.List;

public interface SortedGroupPartitioner
{
  ArrayList<StartAndEnd> computeBoundaries(List<String> columns);

  ArrayList<RowsAndColumns> partitionOnBoundaries(List<String> partitionColumns);
}
