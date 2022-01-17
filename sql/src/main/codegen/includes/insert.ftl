SqlNode DruidSqlInsert() :
{
 SqlNode insertNode;
 SqlNode partitionBy = null;
 SqlNodeList clusterBy = null;
}
{
    insertNode = SqlInsert()
    [
      <PARTITION> <BY>
      partitionBy = StringLiteral()
    ]
    [
      <CLUSTER> <BY>
      clusterBy = ClusterItems()
    ]
    {
  if(partitionBy == null && clusterBy == null) {
    return insertNode;
  }
  if(!(insertNode instanceof SqlInsert)) {
    return insertNode;
  }
  SqlInsert sqlInsert = (SqlInsert) insertNode;
  return new DruidSqlInsert(sqlInsert, partitionBy, clusterBy);
}
}

SqlNodeList ClusterItems() :
{
  List<SqlNode> list;
  final Span s;
  SqlNode e;
}
{
  e = OrderItem() {
    s = span();
    list = startList(e);
  }
  (
    LOOKAHEAD(2) <COMMA> e = OrderItem() { list.add(e); }
  )*
  {
    return new SqlNodeList(list, s.addAll(list).pos());
  }
}