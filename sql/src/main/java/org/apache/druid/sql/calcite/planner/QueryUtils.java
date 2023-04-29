package org.apache.druid.sql.calcite.planner;

import org.apache.calcite.util.Pair;
import org.apache.druid.sql.calcite.rel.DruidQuery;

import java.util.ArrayList;
import java.util.List;

public class QueryUtils
{

  private QueryUtils()
  {
  }

  public static List<ColumnMapping> buildColumnMappings(
      final List<Pair<Integer, String>> fieldMapping,
      final DruidQuery druidQuery
  )
  {
    final List<ColumnMapping> columnMappings = new ArrayList<>();
    for (final Pair<Integer, String> entry : fieldMapping) {
      final String queryColumn = druidQuery.getOutputRowSignature().getColumnName(entry.getKey());
      final String outputColumns = entry.getValue();
      columnMappings.add(new ColumnMapping(queryColumn, outputColumns));
    }

    return columnMappings;
  }
}
