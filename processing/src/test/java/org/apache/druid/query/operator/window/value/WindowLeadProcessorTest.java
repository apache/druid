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

public class WindowLeadProcessorTest
{
  @Test
  public void testLeadProcessing()
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
        new WindowLeadProcessor("intCol", "LeadingIntCol", 2),
        new WindowLeadProcessor("doubleCol", "LeadingDoubleCol", 4),
        new WindowLeadProcessor("objectCol", "LeadingObjectCol", 1)
    );

    final RowsAndColumns results = processor.process(rac);
    RowsAndColumnsHelper.assertEquals(results, "intCol", new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    RowsAndColumnsHelper.assertEquals(results, "doubleCol", new double[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

    final RowsAndColumnsHelper helper = new RowsAndColumnsHelper(results);
    helper.forColumn("LeadingIntCol", ColumnType.LONG)
          .setExpectation(new int[]{2, 3, 4, 5, 6, 7, 8, 9, 0, 0})
          .setNulls(new int[]{8, 9})
          .validate();

    helper.forColumn("LeadingDoubleCol", ColumnType.DOUBLE)
          .setExpectation(new double[]{4, 5, 6, 7, 8, 9, 0, 0, 0, 0})
          .setNulls(new int[]{6, 7, 8, 9})
          .validate();

    helper.forColumn("LeadingObjectCol", ColumnType.STRING)
          .setExpectation(new String[]{"b", "c", "d", "e", "f", "g", "h", "i", "j", null})
          .setNulls(new int[]{9})
          .validate();
  }
}