/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Taken from syntax of SqlInsert statement from calcite parser, edited for replace syntax
SqlNode DruidSqlReplace() :
{
    SqlNode table;
    SqlNodeList extendList = null;
    SqlNode source;
    SqlNodeList columnList = null;
    final Span s;
    SqlInsert sqlInsert;
    // Using fully qualified name for Pair class, since Calcite also has a same class name being used in the Parser.jj
    org.apache.druid.java.util.common.Pair<Granularity, String> partitionedBy = new org.apache.druid.java.util.common.Pair(null, null);
    List<String> partitionSpecList;
    final Pair<SqlNodeList, SqlNodeList> p;
}
{
    <REPLACE> { s = span(); }
    <INTO> table = CompoundIdentifier()
    [
        p = ParenthesizedCompoundIdentifierList() {
            if (p.right.size() > 0) {
                table = extend(table, p.right);
            }
            if (p.left.size() > 0) {
                columnList = p.left;
            }
        }
    ]
    source = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    <FOR> partitionSpecList = PartitionSpecs()
    [
      <PARTITIONED> <BY>
      partitionedBy = PartitionGranularity()
    ]
    {
        sqlInsert = new SqlInsert(s.end(source), SqlNodeList.EMPTY, table, source, columnList);
        return new DruidSqlReplace(sqlInsert, partitionedBy.lhs, partitionedBy.rhs, partitionSpecList);
    }
}

List<String> PartitionSpecs() :
{
  List<String> partitionSpecList;
  String intervalString;
}
{
  (
    <ALL> <TIME>
    {
     return startList("all");
    }
  |
    intervalString = PartitionSpec()
    {
      return startList(intervalString);
    }
  |
    <LPAREN>
    intervalString = PartitionSpec()
    {
      partitionSpecList = startList(intervalString);
    }
    (
      <COMMA>
      intervalString = PartitionSpec()
      {
        partitionSpecList.add(intervalString);
      }
    )*
    <RPAREN>
    {
      return partitionSpecList;
    }
  )
}

String PartitionSpec() :
{
  final Span s;
  SqlNode sqlNode;
}
{
  (
    <PARTITION> sqlNode = StringLiteral()
    {
      return SqlParserUtil.parseString(SqlLiteral.stringValue(sqlNode));
    }
  |
    <PARTITION> <TIMESTAMP> { s = span(); } <QUOTED_STRING>
    {
      SqlTimestampLiteral timestampLiteral = SqlParserUtil.parseTimestampLiteral(token.image, s.end(this));
      return timestampLiteral.toFormattedString();
    }
  )
}
