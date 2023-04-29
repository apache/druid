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

// Taken from syntax of SqlInsert statement from calcite parser, edited for delete syntax
SqlNode DruidSqlDeleteEof() :
{
    SqlNode table;
    SqlNode deleteCondition = null;
    final Span s;
    // Using fully qualified name for Pair class, since Calcite also has a same class name being used in the Parser.jj
    org.apache.druid.java.util.common.Pair<Granularity, String> partitionedBy = new org.apache.druid.java.util.common.Pair(null, null);
    SqlNodeList clusteredBy = null;
}
{
    <DELETE> { s = span(); }
    <FROM>
    table = CompoundIdentifier()
    [
      <WHERE> deleteCondition = Expression(ExprContext.ACCEPT_SUB_QUERY)
      { s.add(deleteCondition); }
    ]
    [
      <PARTITIONED> <BY>
      partitionedBy = PartitionGranularity()
    ]
    [
      <CLUSTERED> <BY>
      clusteredBy = ClusterItems()
    ]
    {
        // In the future, if the existing partitioning and clustering could be infered, these parameters could be made optional.
        if (clusteredBy != null && partitionedBy.lhs == null) {
          throw new ParseException("CLUSTERED BY found before PARTITIONED BY. In Druid, the CLUSTERED BY clause must follow the PARTITIONED BY clause");
        }
        if (clusteredBy != null) {
          s.addAll(clusteredBy);
        }
    }
    // EOF is also present in SqlStmtEof but EOF is a special case and a single EOF can be consumed multiple times.
    // The reason for adding EOF here is to ensure that we create a DruidSqlReplace node after the syntax has been
    // validated and throw SQL syntax errors before performing validations in the DruidSqlReplace which can overshadow the
    // actual error message.
    <EOF>
    {
        if (deleteCondition == null) {
          throw new ParseException("WHERE clause must be present in DELETE statements. (To delete all rows, WHERE TRUE must be explicitly specified.)");
        }
        return DruidSqlDelete.create(
          s.pos(),
          table,
          deleteCondition,
          partitionedBy.lhs,
          partitionedBy.rhs,
          clusteredBy
        );
    }
}

