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
    final SqlIdentifier destination;
    SqlNode source;
    SqlNodeList columnList = null;
    final Span s;
    SqlNode tableRef = null;
    SqlInsert sqlInsert;
    SqlGranularityLiteral partitionedBy = null;
    SqlNodeList clusteredBy = null;
    final Pair<SqlNodeList, SqlNodeList> p;
    SqlNode replaceTimeQuery = null;
    SqlIdentifier exportFileFormat = null;
}
{
    <REPLACE> { s = span(); }
    <INTO>
    (
      LOOKAHEAD(2)
      <EXTERN> <LPAREN> destination = ExternalDestination() <RPAREN>
      |
      destination = CompoundTableIdentifier()
      ( tableRef = TableHints(destination) | { tableRef = destination; } )
      [ LOOKAHEAD(5) tableRef = ExtendTable(tableRef) ]
    )
    (
      LOOKAHEAD(2)
      p = ParenthesizedCompoundIdentifierList() {
        if (p.right.size() > 0) {
          tableRef = extend(tableRef, p.right);
        }
        if (p.left.size() > 0) {
          columnList = p.left;
        } else {
          columnList = null;
        }
      }
      | { columnList = null; }
    )
    [
      <AS> exportFileFormat = FileFormat()
    ]
    [
	    <OVERWRITE>
	    [
		    replaceTimeQuery = ReplaceTimeQuery()
	    ]
    ]
    source = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    // PARTITIONED BY is necessary, but is kept optional in the grammar. It is asserted that it is not missing in the
    // IngestHandler#validate() so that we can return a custom error message.
    [
      <PARTITIONED> <BY>
      partitionedBy = PartitionGranularity()
    ]
    [
      clusteredBy = ClusteredBy()
    ]
    {
        if (clusteredBy != null && partitionedBy == null) {
          throw org.apache.druid.sql.calcite.parser.DruidSqlParserUtils.problemParsing(
            "CLUSTERED BY found before PARTITIONED BY, CLUSTERED BY must come after the PARTITIONED BY clause"
          );
        }
    }
    // EOF is also present in SqlStmtEof but EOF is a special case and a single EOF can be consumed multiple times.
    // The reason for adding EOF here is to ensure that we create a DruidSqlReplace node after the syntax has been
    // validated and throw SQL syntax errors before performing validations in the DruidSqlReplace which can overshadow the
    // actual error message.
    <EOF>
    {
      sqlInsert = new SqlInsert(s.end(source), SqlNodeList.EMPTY, destination, source, columnList);
      return DruidSqlReplace.create(sqlInsert, partitionedBy, clusteredBy, exportFileFormat, replaceTimeQuery);
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
      replaceQuery = Where()
    )
    {
      return replaceQuery;
    }
}
