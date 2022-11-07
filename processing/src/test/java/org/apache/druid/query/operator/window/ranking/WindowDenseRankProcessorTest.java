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

public class WindowDenseRankProcessorTest
{
  @Test
  public void testDenseRankProcessing()
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("vals", new IntArrayColumn(new int[]{7, 18, 18, 30, 120, 121, 122, 122, 8290, 8290}));

    MapOfColumnsRowsAndColumns rac = MapOfColumnsRowsAndColumns.fromMap(map);

    Processor processor = new WindowDenseRankProcessor(Collections.singletonList("vals"), "DenseRank");

    final RowsAndColumns results = processor.process(rac);
    RowsAndColumnsHelper.assertEquals(results, "vals", new int[]{7, 18, 18, 30, 120, 121, 122, 122, 8290, 8290});

    final RowsAndColumnsHelper helper = new RowsAndColumnsHelper(results);
    helper.forColumn("DenseRank", ColumnType.LONG)
          .setExpectation(new int[]{1, 2, 2, 3, 4, 5, 6, 6, 7, 7})
          .validate();
  }
}