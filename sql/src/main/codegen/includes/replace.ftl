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

// Using fully qualified name for Pair class, since Calcite also has a same class name being used in the Parser.jj
SqlNode DruidSqlReplace() :
{
    final List<SqlLiteral> keywords = new ArrayList<SqlLiteral>();
    final SqlNodeList keywordList;
    SqlNode table;
    SqlNodeList extendList = null;
    SqlNode source;
    SqlNodeList columnList = null;
    final Span s;
    SqlInsert sqlInsert;
    org.apache.druid.java.util.common.Pair<Granularity, String> partitionedBy = new org.apache.druid.java.util.common.Pair(null, null);
    List<String> partitionSpecList;
}
{
    <REPLACE> { s = span(); }
    SqlInsertKeywords(keywords)
    {
        keywordList = new SqlNodeList(keywords, s.addAll(keywords).pos());
    }
    <INTO> table = CompoundIdentifier()
    <FOR> partitionSpecList = PartitionSpecs()
    [
        LOOKAHEAD(5)
        [ <EXTEND> ]
        extendList = ExtendList() {
            table = extend(table, extendList);
        }
    ]
    [
        LOOKAHEAD(2)
        { final Pair<SqlNodeList, SqlNodeList> p; }
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
    [
      <PARTITIONED> <BY>
      partitionedBy = PartitionGranularity()
    ]
    {
        sqlInsert = new SqlInsert(s.end(source), keywordList, table, source, columnList);
        return new DruidSqlReplace(sqlInsert, partitionedBy.lhs, partitionedBy.rhs, partitionSpecList);
    }
}
