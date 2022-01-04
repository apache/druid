SqlNode DruidSqlSelect() :
{
 org.apache.calcite.sql.SqlNode stmt;
 org.apache.calcite.sql.SqlNode selectNode;
 org.apache.calcite.sql.SqlNodeList contextParameters = null;
}
{
    selectNode = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    [
      <SET> <CONTEXT>
      contextParameters = ParenthesizedKeyValueOptionCommaList()
    ]
    {
      if(contextParameters != null) {
        if(selectNode instanceof org.apache.calcite.sql.SqlSelect) {
          return new org.apache.druid.sql.calcite.parser.DruidSqlSelect((org.apache.calcite.sql.SqlSelect) selectNode, contextParameters);
        } else if(selectNode instanceof org.apache.calcite.sql.SqlOrderBy) {
          return new org.apache.druid.sql.calcite.parser.DruidSqlOrderBy((org.apache.calcite.sql.SqlOrderBy) selectNode, contextParameters);
        }
      }
      return selectNode;
    }
}


/**
* The following has been directly copied from a later version of the Calcite's Parser.jj. This should be removed while
* upgrading the Calcite's version
*/

SqlNodeList ParenthesizedKeyValueOptionCommaList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    { s = span(); }
    <LPAREN>
    KeyValueOption(list)
    (
        <COMMA>
        KeyValueOption(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

/**
* Parses an option with format key=val whose key is a simple identifier or string literal
* and value is a string literal.
*/
void KeyValueOption(List<SqlNode> list) :
{
    final SqlNode key;
    final SqlNode value;
}
{
    (
        key = SimpleIdentifier()
    |
        key = StringLiteral()
    )
    <EQ>
    value = StringLiteral() {
        list.add(key);
        list.add(value);
    }
}