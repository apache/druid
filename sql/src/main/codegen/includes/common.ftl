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
SqlNode PartitionGranularity() :
{
  SqlNode e;
  SqlNode result;
}
{
  (
    <HOUR>
    {
      result = SqlLiteral.createCharString(DruidSqlParserUtils.HOUR_GRAIN, getPos());
    }
  |
    <DAY>
    {
      result = SqlLiteral.createCharString(DruidSqlParserUtils.DAY_GRAIN, getPos());
    }
  |
    <MONTH>
    {
      result = SqlLiteral.createCharString(DruidSqlParserUtils.MONTH_GRAIN, getPos());
    }
  |
    <YEAR>
    {
      result = SqlLiteral.createCharString(DruidSqlParserUtils.YEAR_GRAIN, getPos());
    }
  |
    <ALL>
    {
      result = SqlLiteral.createCharString(DruidSqlParserUtils.ALL_GRAIN, getPos());
    }
    [
      <TIME>
      {
        result = SqlLiteral.createCharString(DruidSqlParserUtils.ALL_TIME_GRAIN, getPos());
      }
    ]
  |
    e = Expression(ExprContext.ACCEPT_SUB_QUERY)
    {
      result = e;
    }
  )
  {
    return result;
  }
}

SqlNodeList ClusteredBy() :
{
  final List<SqlNode> list = new ArrayList<SqlNode>();
  final Span s;
  SqlNode e;
}
{
  <CLUSTERED> {
    s = span();
  }
  <BY> AddOrderItem(list)
  (
    LOOKAHEAD(2) <COMMA> AddOrderItem(list)
  )*
  {
    return new SqlNodeList(list, s.addAll(list).pos());
  }
}

SqlTypeNameSpec DruidType() :
{
  String typeName;
}
{
  <TYPE> <LPAREN> <QUOTED_STRING>
  {
    typeName = SqlParserUtil.trim(token.image, "'");
  }
  <RPAREN>
  {
    return new SqlUserDefinedTypeNameSpec(typeName, span().pos());
  }
}
