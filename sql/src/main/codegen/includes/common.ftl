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
      unparseString = e.toString();
    }
  )
  {
    return new org.apache.druid.java.util.common.Pair(granularity, unparseString);
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

// Parses the supported file formats for export.
String FileFormat() :
{}
{
  (
    <CSV>
    {
      return "CSV";
    }
  )
}

SqlIdentifier ExternalDestination() :
{
  final Span s;
  Map<String, String> properties = new HashMap();
}
{
  (
    <S3> [ <LPAREN> [properties = ExternProperties()] <RPAREN>]
    {
      s = span();
      return new ExternalDestinationSqlIdentifier(
        "s3",
        s.pos(),
        properties
      );
    }
    |
    <LOCAL> [ <LPAREN> [properties = ExternProperties()] <RPAREN>]
    {
      s = span();
      return new ExternalDestinationSqlIdentifier(
        "local",
        s.pos(),
        properties
      );
    }
  )
}

Map<String, String> ExternProperties() :
{
  final Span s;
  final Map<String, String> properties = new HashMap();
  SqlNodeList commaList = SqlNodeList.EMPTY;
}
{
  commaList = ExpressionCommaList(span(), ExprContext.ACCEPT_NON_QUERY)
  {
    for (SqlNode sqlNode : commaList) {
      List<SqlNode> sqlNodeList = ((SqlBasicCall) sqlNode).getOperandList();
      properties.put(((SqlIdentifier) sqlNodeList.get(0)).getSimple(), ((SqlIdentifier) sqlNodeList.get(1)).getSimple());
    }
    return properties;
  }
}
