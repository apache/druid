package org.apache.druid.query.operator.window.ranking;

import org.apache.druid.query.operator.window.Processor;
import org.apache.druid.query.operator.window.RowsAndColumnsHelper;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.apache.druid.query.rowsandcols.frame.MapOfColumnsRowsAndColumns;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class WindowCumeDistProcessorTest
{
  @Test
  public void testCumeDistProcessing()
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("vals", new IntArrayColumn(new int[]{7, 18, 18, 30, 120, 121, 122, 122, 8290, 8290}));

    MapOfColumnsRowsAndColumns rac = MapOfColumnsRowsAndColumns.fromMap(map);

    Processor processor = new WindowCumeDistProcessor(Collections.singletonList("vals"), "CumeDist");

    final RowsAndColumns results = processor.process(rac);
    RowsAndColumnsHelper.assertEquals(results, "vals", new int[]{7, 18, 18, 30, 120, 121, 122, 122, 8290, 8290});

    final RowsAndColumnsHelper helper = new RowsAndColumnsHelper(results);
    helper.forColumn("CumeDist", ColumnType.DOUBLE)
          .setExpectation(new double[]{0.1, 0.3, 0.3, 0.4, 0.5, 0.6, 0.8, 0.8, 1.0, 1.0})
          .validate();
  }
}