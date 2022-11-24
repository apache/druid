package org.apache.druid.query.operator;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.rowsandcols.DefaultSortedGroupPartitioner;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.SortedGroupPartitioner;

import java.util.Iterator;
import java.util.List;

/**
 * This naive partitioning operator assumes that it's child operator always gives it RowsAndColumns objects that are
 * a superset of the partitions that it needs to provide.  It will never attempt to make a partition larger than a
 * single RowsAndColumns object that it is given from its child Operator.  A different operator should be used
 * if that is an important bit of functionality to have.
 */
public class NaivePartitioningOperator implements Operator
{
  private final List<String> partitionColumns;
  private final Operator child;

  private Iterator<RowsAndColumns> partitionsIter;

  public NaivePartitioningOperator(
      List<String> partitionColumns,
      Operator child
  )
  {
    this.partitionColumns = partitionColumns;
    this.child = child;
  }

  @Override
  public void open()
  {
    child.open();
  }

  @Override
  public RowsAndColumns next()
  {
    if (partitionsIter != null && partitionsIter.hasNext()) {
      return partitionsIter.next();
    }

    if (child.hasNext()) {
      final RowsAndColumns rac = child.next();

      SortedGroupPartitioner groupPartitioner = rac.as(SortedGroupPartitioner.class);
      if (groupPartitioner == null) {
        groupPartitioner = new DefaultSortedGroupPartitioner(rac);
      }

      partitionsIter = groupPartitioner.partitionOnBoundaries(partitionColumns).iterator();
      return partitionsIter.next();
    }

    throw new ISE("Asked for next when already complete");
  }

  @Override
  public boolean hasNext()
  {
    if (partitionsIter != null && partitionsIter.hasNext()) {
      return true;
    }

    if (child.hasNext()) {
      return true;
    }

    return false;
  }

  @Override
  public void close(boolean cascade)
  {
    if (cascade) {
      child.close(cascade);
    }
  }
}
