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
  final List<SqlNode> columns = new ArrayList<SqlNode>();
  final List<SqlNode> projections = new ArrayList<SqlNode>();
  Span elementSpan = null;
  SqlGranularityLiteral partitionedBy = null;
  SqlNodeList clusteredBy = null;
  boolean sealed = false;
}
{
  <TABLE>
  [ <IF> <NOT> <EXISTS> { ifNotExists = true; } ]
  id = CompoundTableIdentifier()
  [
    // SEALED binds to the column list rather than trailing the statement: it declares that the list is the table's
    // whole schema, so it is only accepted when there is a list for it to describe.
    [ <SEALED> { sealed = true; } ]
    <LPAREN> { elementSpan = span(); }
    AddDruidTableElement(columns, projections)
    (
      <COMMA> AddDruidTableElement(columns, projections)
    )*
    <RPAREN>
  ]
  [
    <PARTITIONED> <BY>
    partitionedBy = PartitionGranularity()
  ]
  [
    clusteredBy = ClusteredBy()
  ]
  {
    final SqlParserPos elementPos = elementSpan == null ? s.pos() : elementSpan.end(this);
    return new DruidSqlCreateTable(
        s.end(this),
        replace,
        ifNotExists,
        id,
        new SqlNodeList(columns, elementPos),
        new SqlNodeList(projections, elementPos),
        partitionedBy,
        clusteredBy,
        sealed
    );
  }
}

// A table element is either a column declaration or a projection definition. A column may legitimately be named
// "projection" (the keyword is non-reserved) and may have a bare-identifier type, so two tokens are not enough to
// tell the two apart: a projection definition is distinguished by its third token, which is always '(' or AS.
void AddDruidTableElement(List<SqlNode> columns, List<SqlNode> projections) :
{
  final DruidSqlColumnDeclaration column;
  final SqlProjectionSpec projection;
}
{
  LOOKAHEAD(3)
  projection = DruidProjectionDefinition()
  {
    projections.add(projection);
  }
|
  column = DruidColumnDeclaration()
  {
    columns.add(column);
  }
}

// The body is a SELECT with no FROM: the table the projection belongs to is implicit. Only a select list, WHERE and
// GROUP BY are admitted; a projection has no way to express ordering, limits or having.
SqlProjectionSpec DruidProjectionDefinition() :
{
  final Span s;
  final Span bodySpan;
  final SqlIdentifier name;
  final List<SqlLiteral> keywords = new ArrayList<SqlLiteral>();
  final SqlNodeList keywordList;
  final List<SqlNode> selectList = new ArrayList<SqlNode>();
  SqlLiteral keyword = null;
  final SqlNode where;
  final SqlNodeList groupBy;
  SqlNodeList clusteredBy = null;
}
{
  <PROJECTION> { s = span(); }
  name = SimpleIdentifier()
  [ <AS> ]
  <LPAREN>
  <SELECT> { bodySpan = span(); }
  [ keyword = AllOrDistinct() { keywords.add(keyword); } ]
  { keywordList = new SqlNodeList(keywords, bodySpan.addAll(keywords).pos()); }
  AddSelectItem(selectList)
  (
    <COMMA> AddSelectItem(selectList)
  )*
  ( where = Where() | { where = null; } )
  ( groupBy = GroupBy() | { groupBy = null; } )
  // Only meaningful for the reserved __base projection, where it names the columns segments are clustered on.
  [ clusteredBy = ClusteredBy() ]
  <RPAREN>
  {
    return new SqlProjectionSpec(
        s.end(this),
        name,
        clusteredBy,
        new SqlSelect(
            bodySpan.end(this),
            keywordList,
            new SqlNodeList(selectList, Span.of(selectList).pos()),
            null,
            where,
            groupBy,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        )
    );
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
  final SqlProjectionSpec projection;
  final SqlIdentifier projectionName;
  boolean ifNotExists = false;
  boolean ifExists = false;
}
{
  <ALTER> { s = span(); } <TABLE> id = CompoundTableIdentifier()
  (
    <ADD>
    (
      <COLUMN> column = DruidColumnDeclaration()
      {
        return new DruidSqlAlterTable.AddColumn(s.end(this), id, column);
      }
    |
      [ <IF> <NOT> <EXISTS> { ifNotExists = true; } ] projection = DruidProjectionDefinition()
      {
        return new DruidSqlAlterTable.AddProjection(s.end(this), id, projection, ifNotExists);
      }
    )
  |
    <DROP>
    (
      <COLUMN> columnName = SimpleIdentifier()
      {
        return new DruidSqlAlterTable.DropColumn(s.end(this), id, columnName);
      }
    |
      <PROJECTION> [ <IF> <EXISTS> { ifExists = true; } ] projectionName = SimpleIdentifier()
      {
        return new DruidSqlAlterTable.DropProjection(s.end(this), id, projectionName, ifExists);
      }
    )
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
