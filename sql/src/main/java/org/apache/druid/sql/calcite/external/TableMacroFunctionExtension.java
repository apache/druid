package org.apache.druid.sql.calcite.external;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNodeList;

public interface TableMacroFunctionExtension
{
  SqlBasicCall rewriteCall(SqlBasicCall oldCall, SqlNodeList schema);
}
