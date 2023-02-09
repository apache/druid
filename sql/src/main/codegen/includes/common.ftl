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
org.apache.druid.java.util.common.Pair<Granularity, String> PartitionGranularity() :
{
  SqlNode e;
  Granularity granularity;
  String unparseString;
}
{
  (
    <HOUR>
    {
      granularity = Granularities.HOUR;
      unparseString = "HOUR";
    }
  |
    <DAY>
    {
      granularity = Granularities.DAY;
      unparseString = "DAY";
    }
  |
    <MONTH>
    {
      granularity = Granularities.MONTH;
      unparseString = "MONTH";
    }
  |
    <YEAR>
    {
      granularity = Granularities.YEAR;
      unparseString = "YEAR";
    }
  |
    <ALL>
    {
      granularity = Granularities.ALL;
      unparseString = "ALL";
    }
    [
      <TIME>
      {
        unparseString += " TIME";
      }
    ]
  |
    e = Expression(ExprContext.ACCEPT_SUB_QUERY)
    {
      granularity = DruidSqlParserUtils.convertSqlNodeToGranularityThrowingParseExceptions(e);
      DruidSqlParserUtils.throwIfUnsupportedGranularityInPartitionedBy(granularity);
      unparseString = e.toString();
    }
  )
  {
    return new org.apache.druid.java.util.common.Pair(granularity, unparseString);
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
