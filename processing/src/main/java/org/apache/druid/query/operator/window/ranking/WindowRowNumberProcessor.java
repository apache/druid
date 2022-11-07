package org.apache.druid.query.operator.window.ranking;

import org.apache.druid.query.operator.window.Processor;
import org.apache.druid.query.rowsandcols.AppendableRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.query.rowsandcols.column.ColumnAccessorBasedColumn;
import org.apache.druid.segment.column.ColumnType;

public class WindowRowNumberProcessor implements Processor
{
  private final String outputColumn;

  public WindowRowNumberProcessor(
      String outputColumn
  ) {
    this.outputColumn = outputColumn;
  }

  @Override
  public RowsAndColumns process(RowsAndColumns incomingPartition)
  {
    final AppendableRowsAndColumns retVal = RowsAndColumns.expectAppendable(incomingPartition);
    return retVal.addColumn(
        outputColumn,
        new ColumnAccessorBasedColumn(
            new ColumnAccessor()
            {
              @Override
              public ColumnType getType()
              {
                return ColumnType.LONG;
              }

              @Override
              public int numCells()
              {
                return incomingPartition.numRows();
              }

              @Override
              public boolean isNull(int cell)
              {
                return false;
              }

              @Override
              public Object getObject(int cell)
              {
                return getInt(cell);
              }

              @Override
              public double getDouble(int cell)
              {
                return getInt(cell);
              }

              @Override
              public float getFloat(int cell)
              {
                return getInt(cell);
              }

              @Override
              public long getLong(int cell)
              {
                return getInt(cell);
              }

              @Override
              public int getInt(int cell)
              {
                // cell is 0-indexed, rowNumbers are 1-indexed, so add 1.
                return cell + 1;
              }

              @Override
              public int compareCells(int lhsCell, int rhsCell)
              {
                return Integer.compare(lhsCell, rhsCell);
              }
            }
        )
    );
  }
}
