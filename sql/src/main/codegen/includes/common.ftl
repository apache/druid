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

SqlGranularityLiteral PartitionGranularity() :
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
      granularity = DruidSqlParserUtils.convertSqlNodeToGranularity(e);
      unparseString = e.toString();
    }
  )
  {
    return new SqlGranularityLiteral(granularity, unparseString, getPos());
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
SqlIdentifier FileFormat() :
{
  SqlNode format;
}
{
  format = SimpleIdentifier()
  {
    return (SqlIdentifier) format;
  }
}

SqlIdentifier ExternalDestination() :
{
  final Span s;
  SqlIdentifier destinationType = null;
  String destinationTypeString = null;
  Map<String, String> properties = new HashMap();
}
{
 (
  destinationType = SimpleIdentifier()
  {
    destinationTypeString = destinationType.toString();
  }
  |
  <LOCAL>
  {
    // local is a reserved keyword in calcite. However, local is also a supported input source / destination and
    // keeping the name is preferred for consistency in other places, and so that permission checks are applied
    // correctly, so this is handled as a special case.
    destinationTypeString = "local";
  }
 )
 [ <LPAREN> [ properties = ExternProperties() ] <RPAREN>]
 {
   s = span();
   return new ExternalDestinationSqlIdentifier(
     destinationTypeString,
     s.pos(),
     properties
   );
 }
}

Map<String, String> ExternProperties() :
{
  final Span s;
  final Map<String, String> properties = new HashMap();
  SqlIdentifier identifier;
  String value;
  SqlNodeList commaList = SqlNodeList.EMPTY;
}
{
  (
    identifier = SimpleIdentifier() <NAMED_ARGUMENT_ASSIGNMENT> value = SimpleStringLiteral()
    {
      properties.put(identifier.toString(), value);
    }
  )
  (
    <COMMA>
    identifier = SimpleIdentifier() <NAMED_ARGUMENT_ASSIGNMENT> value = SimpleStringLiteral()
    {
      properties.put(identifier.toString(), value);
    }
  )*
  {
    return properties;
  }
}

SqlNode testRule():
{
  final SqlNode e;
}
{
  e = SimpleIdentifier() { return e; }
}