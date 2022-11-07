package org.apache.druid.query.operator.window;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.DoubleArrayColumn;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.apache.druid.query.rowsandcols.column.ObjectArrayColumn;
import org.apache.druid.query.rowsandcols.frame.MapOfColumnsRowsAndColumns;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class WindowAggregateProcessorTest
{
  static {
    NullHandling.initializeForTests();
  }

  @Test
  public void testAggregation()
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("intCol", new IntArrayColumn(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    map.put("doubleCol", new DoubleArrayColumn(new double[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    map.put("objectCol", new ObjectArrayColumn(
        new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
        ColumnType.STRING)
    );

    MapOfColumnsRowsAndColumns rac = MapOfColumnsRowsAndColumns.fromMap(map);

    WindowAggregateProcessor processor = new WindowAggregateProcessor(
        Arrays.asList(
            new LongSumAggregatorFactory("sumFromLong", "intCol"),
            new LongSumAggregatorFactory("sumFromDouble", "doubleCol"),
            new DoubleMaxAggregatorFactory("maxFromInt", "intCol"),
            new DoubleMaxAggregatorFactory("maxFromDouble", "doubleCol")
        ),
        Arrays.asList(
            new LongMaxAggregatorFactory("cummMax", "intCol"),
            new DoubleSumAggregatorFactory("cummSum", "doubleCol")
        )
    );

    final RowsAndColumns results = processor.process(rac);
    RowsAndColumnsHelper.assertEquals(results, "intCol", new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    RowsAndColumnsHelper.assertEquals(results, "doubleCol", new double[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

    RowsAndColumnsHelper.assertEquals(results, "sumFromLong", new long[]{45, 45, 45, 45, 45, 45, 45, 45, 45, 45});
    RowsAndColumnsHelper.assertEquals(results, "sumFromDouble", new long[]{45, 45, 45, 45, 45, 45, 45, 45, 45, 45});
    RowsAndColumnsHelper.assertEquals(results, "maxFromInt", new double[]{9, 9, 9, 9, 9, 9, 9, 9, 9, 9});
    RowsAndColumnsHelper.assertEquals(results, "maxFromDouble", new double[]{9, 9, 9, 9, 9, 9, 9, 9, 9, 9});
    RowsAndColumnsHelper.assertEquals(results, "cummMax", new long[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    RowsAndColumnsHelper.assertEquals(results, "cummSum", new double[]{0, 1, 3, 6, 10, 15, 21, 28, 36, 45});
  }

}