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
SqlNode DruidSqlReplaceEof() :
{
    SqlNode table;
    SqlNode source;
    SqlNodeList columnList = null;
    final Span s;
    SqlInsert sqlInsert;
    // Using fully qualified name for Pair class, since Calcite also has a same class name being used in the Parser.jj
    org.apache.druid.java.util.common.Pair<Granularity, String> partitionedBy = new org.apache.druid.java.util.common.Pair(null, null);
    SqlNodeList clusteredBy = null;
    final Pair<SqlNodeList, SqlNodeList> p;
    SqlNode replaceTimeQuery = null;
}
{
    <REPLACE> { s = span(); }
    <INTO>
    table = CompoundIdentifier()
    [
        p = ParenthesizedCompoundIdentifierList() {
            if (p.left.size() > 0) {
                columnList = p.left;
            }
        }
    ]
    [
        <OVERWRITE> replaceTimeQuery = ReplaceTimeQuery()
    ]
    source = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    // PARTITIONED BY is necessary, but is kept optional in the grammar. It is asserted that it is not missing in the
    // DruidSqlInsert constructor so that we can return a custom error message.
    [
      <PARTITIONED> <BY>
      partitionedBy = PartitionGranularity()
    ]
    [
      <CLUSTERED> <BY>
      clusteredBy = ClusterItems()
    ]
    {
        if (clusteredBy != null && partitionedBy.lhs == null) {
          throw new ParseException("CLUSTERED BY found before PARTITIONED BY. In Druid, the CLUSTERED BY clause must follow the PARTITIONED BY clause");
        }
    }
    // EOF is also present in SqlStmtEof but EOF is a special case and a single EOF can be consumed multiple times.
    // The reason for adding EOF here is to ensure that we create a DruidSqlReplace node after the syntax has been
    // validated and throw SQL syntax errors before performing validations in the DruidSqlReplace which can overshadow the
    // actual error message.
    <EOF>
    {
        sqlInsert = new SqlInsert(s.end(source), SqlNodeList.EMPTY, table, source, columnList);
        return new DruidSqlReplace(sqlInsert, partitionedBy.lhs, partitionedBy.rhs, clusteredBy, replaceTimeQuery);
    }
}

SqlNode ReplaceTimeQuery() :
{
    SqlNode replaceQuery;
}
{
    (
      <ALL> { replaceQuery = SqlLiteral.createCharString("ALL", getPos()); }
    |
      // We parse all types of conditions and throw an exception if it is not supported to keep the parsing simple
      replaceQuery = WhereOpt()
    )
    {
      return replaceQuery;
    }
}
