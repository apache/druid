package org.apache.druid.query.operator.window;

import org.apache.druid.query.rowsandcols.RowsAndColumns;

public interface Processor
{
   RowsAndColumns process(RowsAndColumns incomingPartition);
}
