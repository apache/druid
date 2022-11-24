package org.apache.druid.query.operator.window.value;

import org.apache.druid.query.operator.window.ComposingProcessor;
import org.apache.druid.query.operator.window.RowsAndColumnsHelper;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.DoubleArrayColumn;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.apache.druid.query.rowsandcols.column.ObjectArrayColumn;
import org.apache.druid.query.rowsandcols.frame.MapOfColumnsRowsAndColumns;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

public class WindowLagProcessorTest
{
  @Test
  public void testLagProcessing()
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("intCol", new IntArrayColumn(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    map.put("doubleCol", new DoubleArrayColumn(new double[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    map.put("objectCol", new ObjectArrayColumn(
                new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
                ColumnType.STRING
            )
    );

    MapOfColumnsRowsAndColumns rac = MapOfColumnsRowsAndColumns.fromMap(map);

    ComposingProcessor processor = new ComposingProcessor(
        new WindowLagProcessor("intCol", "laggardIntCol", 2),
        new WindowLagProcessor("doubleCol", "laggardDoubleCol", 4),
        new WindowLagProcessor("objectCol", "laggardObjectCol", 1)
    );

    final RowsAndColumns results = processor.process(rac);
    RowsAndColumnsHelper.assertEquals(results, "intCol", new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    RowsAndColumnsHelper.assertEquals(results, "doubleCol", new double[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

    final RowsAndColumnsHelper helper = new RowsAndColumnsHelper(results);
    helper.forColumn("laggardIntCol", ColumnType.LONG)
          .setExpectation(new int[]{0, 0, 0, 1, 2, 3, 4, 5, 6, 7})
          .setNulls(new int[]{0, 1})
          .validate();

    helper.forColumn("laggardDoubleCol", ColumnType.DOUBLE)
          .setExpectation(new double[]{0, 0, 0, 0, 0, 1, 2, 3, 4, 5})
          .setNulls(new int[]{0, 1, 2, 3})
          .validate();

    helper.forColumn("laggardObjectCol", ColumnType.STRING)
          .setExpectation(new String[]{null, "a", "b", "c", "d", "e", "f", "g", "h", "i"})
          .setNulls(new int[]{0})
          .validate();
  }
}