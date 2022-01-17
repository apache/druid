SqlNode DruidSqlInsert() :
{
 org.apache.calcite.sql.SqlNode insertNode;
 org.apache.calcite.sql.SqlNode partitionBy = null;
 org.apache.calcite.sql.SqlNode clusterBy = null;
}
{
    insertNode = SqlInsert()
    [
      <PARTITION> <BY>
      partitionBy = StringLiteral()
    ]
    [
      <CLUSTER> <BY>
      clusterBy = StringLiteral()
    ]
    {
  if(partitionBy == null && clusterBy == null) {
    return insertNode;
  }
  if(!(insertNode instanceof org.apache.calcite.sql.SqlInsert)) {
    return insertNode;
  }
  org.apache.calcite.sql.SqlInsert sqlInsert = (org.apache.calcite.sql.SqlInsert) insertNode;
  return new org.apache.druid.sql.calcite.parser.DruidSqlInsert(sqlInsert, partitionBy, clusterBy);
}
}
