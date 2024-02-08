package org.apache.druid.sql.calcite.parser;

import java.util.Map;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SqlGranularityLiteral extends SqlLiteral {
  public SqlGranularityLiteral(
      @Nullable Granularity value,
      SqlParserPos pos)
  {
    super(value, SqlTypeName.UNKNOWN, pos);
  }

  @Override
  @Nullable
  public Granularity getValue() {
    return (Granularity) value;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec)
  {
    if (value != null) {
      writer.keyword("PARTITIONED BY");
      writer.keyword(value.toString());
    }
  }
}
