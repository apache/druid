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

// Druid catalog DDL. These statements write catalog metadata only; they do not create or delete data.
//
// CREATE TABLE is reached through the standard Calcite SqlCreate() production, which has already consumed
// CREATE [OR REPLACE], so this production must not consume <EOF>: the enclosing SqlStmtList() handles statement
// separators. ALTER TABLE is a top-level statement production instead, because Calcite's stock SqlAlter() mandates
// a SYSTEM or SESSION scope that does not apply here.

SqlCreate DruidSqlCreateTable(Span s, boolean replace) :
{
  boolean ifNotExists = false;
  final SqlIdentifier id;
  SqlNodeList columnList = SqlNodeList.EMPTY;
  SqlGranularityLiteral partitionedBy = null;
  SqlNodeList clusteredBy = null;
}
{
  <TABLE>
  [ <IF> <NOT> <EXISTS> { ifNotExists = true; } ]
  id = CompoundTableIdentifier()
  [ columnList = DruidColumnDeclarationList() ]
  [
    <PARTITIONED> <BY>
    partitionedBy = PartitionGranularity()
  ]
  [
    clusteredBy = ClusteredBy()
  ]
  {
    return new DruidSqlCreateTable(s.end(this), replace, ifNotExists, id, columnList, partitionedBy, clusteredBy);
  }
}

SqlNodeList DruidColumnDeclarationList() :
{
  final Span s;
  final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
  <LPAREN> { s = span(); }
  AddDruidColumnDeclaration(list)
  (
    <COMMA> AddDruidColumnDeclaration(list)
  )*
  <RPAREN>
  {
    return new SqlNodeList(list, s.end(this));
  }
}

void AddDruidColumnDeclaration(List<SqlNode> list) :
{
  final DruidSqlColumnDeclaration column;
}
{
  column = DruidColumnDeclaration()
  {
    list.add(column);
  }
}

DruidSqlColumnDeclaration DruidColumnDeclaration() :
{
  final SqlIdentifier name;
  final SqlDataTypeSpec dataType;
}
{
  name = SimpleIdentifier()
  dataType = DataType()
  // Nullability is accepted but ignored: all Druid columns are nullable, matching the EXTEND clause of INSERT.
  [ <NOT> <NULL> | <NULL> ]
  {
    return new DruidSqlColumnDeclaration(
        name.getParserPosition().plus(dataType.getParserPosition()),
        name,
        dataType
    );
  }
}

SqlNode DruidSqlAlterTable() :
{
  final Span s;
  final SqlIdentifier id;
  final SqlIdentifier columnName;
  final SqlDataTypeSpec dataType;
  final DruidSqlColumnDeclaration column;
  final SqlNodeList properties;
}
{
  <ALTER> { s = span(); } <TABLE> id = CompoundTableIdentifier()
  (
    <ADD> <COLUMN> column = DruidColumnDeclaration()
    {
      return new DruidSqlAlterTable.AddColumn(s.end(this), id, column);
    }
  |
    <DROP> <COLUMN> columnName = SimpleIdentifier()
    {
      return new DruidSqlAlterTable.DropColumn(s.end(this), id, columnName);
    }
  |
    <ALTER> <COLUMN> columnName = SimpleIdentifier() <SET> <DATA> <TYPE> dataType = DataType()
    {
      return new DruidSqlAlterTable.AlterColumn(
          s.end(this),
          id,
          new DruidSqlColumnDeclaration(
              columnName.getParserPosition().plus(dataType.getParserPosition()),
              columnName,
              dataType
          )
      );
    }
  |
    <SET> <PROPERTIES> properties = DruidPropertyList()
    {
      return new DruidSqlAlterTable.SetProperties(s.end(this), id, properties);
    }
  )
}

SqlNodeList DruidPropertyList() :
{
  final Span s;
  final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
  <LPAREN> { s = span(); }
  AddDruidPropertyAssignment(list)
  (
    <COMMA> AddDruidPropertyAssignment(list)
  )*
  <RPAREN>
  {
    return new SqlNodeList(list, s.end(this));
  }
}

void AddDruidPropertyAssignment(List<SqlNode> list) :
{
  final SqlIdentifier key;
  final SqlNode value;
}
{
  key = SimpleIdentifier() <EQ> value = Literal()
  {
    list.add(
        new DruidSqlPropertyAssignment(
            key.getParserPosition().plus(value.getParserPosition()),
            key,
            value
        )
    );
  }
}
