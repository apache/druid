package org.apache.druid.query.operator.window.ranking;

import org.apache.druid.query.operator.window.ComposingProcessor;
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

public class WindowRankProcessorTest
{
  @Test
  public void testRankProcessing()
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("vals", new IntArrayColumn(new int[]{7, 18, 18, 30, 120, 121, 122, 122, 8290, 8290}));

    MapOfColumnsRowsAndColumns rac = MapOfColumnsRowsAndColumns.fromMap(map);

    Processor processor = new ComposingProcessor(
        new WindowRankProcessor(Collections.singletonList("vals"), "rank", false),
        new WindowRankProcessor(Collections.singletonList("vals"), "rankAsPercent", true)
    );

    final RowsAndColumns results = processor.process(rac);
    RowsAndColumnsHelper.assertEquals(results, "vals", new int[]{7, 18, 18, 30, 120, 121, 122, 122, 8290, 8290});

    final RowsAndColumnsHelper helper = new RowsAndColumnsHelper(results);
    helper.forColumn("rank", ColumnType.LONG)
          .setExpectation(new int[]{1, 2, 2, 4, 5, 6, 7, 7, 9, 9})
          .validate();

    helper.forColumn("rankAsPercent", ColumnType.DOUBLE)
          .setExpectation(new double[]{0.0, 1/9d, 1/9d, 3/9d, 4/9d, 5/9d, 6/9d, 6/9d, 8/9d, 8/9d})
          .validate();
  }
}