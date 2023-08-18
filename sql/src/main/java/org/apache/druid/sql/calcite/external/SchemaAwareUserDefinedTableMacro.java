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

package org.apache.druid.sql.calcite.external;

import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlWriter.Frame;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.expression.AuthorizableOperator;
import org.apache.druid.sql.calcite.table.ExternalTable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Table macro designed for use with the Druid EXTEND operator. Example:
 * <code><pre>
 * INSERT INTO dst
 * SELECT *
 * FROM TABLE(staged(
 *    source => 'inline',
 *    format => 'csv',
 *    data => 'a,b,1
 * c,d,2
 * '
 *   ))
 *   EXTEND (x VARCHAR, y VARCHAR, z BIGINT)
 * PARTITIONED BY ALL TIME
 * </pre></code>
 * <p>
 * Calcite supports the Apache Phoenix EXTEND operator of the form:
 * <code><pre>
 * SELECT ..
 * FROM myTable EXTEND (x VARCHAR, ...)
 * </pre></code>
 * <p>
 * For Druid, we want the above form: extend a table function, not a
 * literal table. Since we can't change the Calcite parser, we instead use
 * tricks within the constraints of the parser.
 * <ul>
 * <li>The Calcite parser is revised to add the
 * EXTEND rule for a table function.</li>
 * <li>Calcite expects the EXTEND operator to have two arguments: an identifier
 * and the column list. Since our case has a function call as the first argument,
 * we can't let Calcite see our AST. So, we use a rewrite trick to convert the
 * EXTEND node into the usual TABLE(.) node, and we modify the associated macro
 * to hold onto the schema, which is now out of sight of Calcite, and so will not
 * cause problems with rules that don't understand our usage.</li>
 * <li>Calcite will helpfully rewrite calls, replacing our modified operator with
 * the original. So, we override those to keep our modified operator.</li>
 * <li>When asked to produce a table ({@code apply(.)}), we call a Druid-specific
 * version that passes along the schema saved previously.</li>
 * <li>The extended {@code DruidTableMacro} uses the schema to define the
 * input source.</li>
 * <li>Care is taken that the same {@code DruidTableMacro} can be used without
 * EXTEND. In this case, the schema will be empty and the input source must have
 * a way of providing the schema. The batch ingest feature does not yet support
 * this use case, but it seems a reasonable extension. Example: CSV that has a
 * header row, or a "classic" lookup table that, by definition, has only two
 * columns.</li>
 * </ul>
 * <p>
 * Note that unparsing is a bit of a nuisance. Our trick places the EXTEND
 * list in the wrong place, and we'll unparse SQL as:
 * <code><pre>
 * FROM TABLE(fn(arg1, arg2) EXTEND (x VARCHAR, ...))
 * </pre></code>
 * Since we seldom use unparse, we can perhaps live with this limitation for now.
 */
public abstract class SchemaAwareUserDefinedTableMacro
    extends BaseUserDefinedTableMacro implements AuthorizableOperator
{
  public SchemaAwareUserDefinedTableMacro(
      SqlIdentifier opName,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandMetadata operandMetadata,
      ExtendedTableMacro tableMacro
  )
  {
    super(opName, returnTypeInference, operandTypeInference, operandMetadata, tableMacro);
  }

  /**
   * Rewrite the call to the original table macro function to a new "shim" version that
   * holds both the original one and the schema from EXTEND.
   */
  public SqlBasicCall rewriteCall(SqlBasicCall oldCall, SqlNodeList schema)
  {
    return new ExtendedCall(oldCall, new ShimUserDefinedTableMacro(this, schema));
  }

  // Note the confusing use of "table macro". A TablMacro is a non-SqlNode that does the
  // actual translation to a table. A *UserDefinedTableMacro is a function that wraps
  // a table macro. The result is that "macro" by itself is ambiguous: it can be the
  // implementation (TableMacro) or the function that wraps the implementation.
  private static class ShimUserDefinedTableMacro extends BaseUserDefinedTableMacro implements AuthorizableOperator
  {
    protected final SchemaAwareUserDefinedTableMacro base;
    protected final SqlNodeList schema;
    private TranslatableTable table;

    public ShimUserDefinedTableMacro(final SchemaAwareUserDefinedTableMacro base, final SqlNodeList schema)
    {
      super(
          base.getNameAsId(),
          ReturnTypes.CURSOR,
          null,
          base.getOperandTypeChecker(),
          new ShimTableMacro((ExtendedTableMacro) base.macro, schema)
      );
      this.base = base;
      this.schema = schema;
    }

    @Override
    public TranslatableTable getTable(SqlOperatorBinding callBinding)
    {
      if (table == null) {
        // Cache the table to avoid multiple conversions
        // Possible because each call has a distinct instance
        // of this operator.
        table = super.getTable(callBinding);
      }
      return table;
    }

    @Override
    public Set<ResourceAction> computeResources(final SqlCall call, final boolean inputSourceTypeSecurityEnabled)
    {
      Set<ResourceAction> resourceActions = new HashSet<>();
      if (table instanceof ExternalTable && inputSourceTypeSecurityEnabled) {
        resourceActions.addAll(((ExternalTable) table)
                                   .getInputSourceTypeSupplier().get().stream()
                                   .map(inputSourceType ->
                                 new ResourceAction(new Resource(inputSourceType, ResourceType.EXTERNAL), Action.READ))
                                   .collect(Collectors.toSet()));
      } else {
        resourceActions.addAll(base.computeResources(call, inputSourceTypeSecurityEnabled));
      }
      return resourceActions;
    }
  }

  /**
   * Call primarily to (nearly) recreate the EXTEND clause during unparse.
   */
  private static class ExtendedCall extends SqlBasicCall
  {
    private final SqlNodeList schema;

    public ExtendedCall(SqlBasicCall oldCall, ShimUserDefinedTableMacro macro)
    {
      super(
          macro,
          oldCall.getOperandList(),
          oldCall.getParserPosition(),
          oldCall.getFunctionQuantifier()
      );
      this.schema = macro.schema;
    }

    public ExtendedCall(ExtendedCall from, SqlParserPos pos)
    {
      super(
          from.getOperator(),
          from.getOperandList(),
          pos,
          from.getFunctionQuantifier()
      );
      this.schema = from.schema;
    }

    /**
     * Politely decline to revise the operator: we want the one we
     * constructed to hold the schema, not the one re-resolved during
     * validation.
     */
    @Override
    public void setOperator(SqlOperator operator)
    {
      // Do nothing: do not call super.setOperator().
    }

    @Override
    public SqlNode clone(SqlParserPos pos)
    {
      return new ExtendedCall(this, pos);
    }

    @Override
    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
      super.unparse(writer, leftPrec, rightPrec);
      writer.keyword("EXTEND");
      Frame frame = writer.startList("(", ")");
      schema.unparse(writer, leftPrec, rightPrec);
      writer.endList(frame);
    }

    /**
     * Required by GHA CodeQL even though Calcite doesn't use this
     * particular method.
     */
    @Override
    public Object clone()
    {
      throw new UOE("Not supported");
    }
  }

  public interface ExtendedTableMacro extends TableMacro
  {
    TranslatableTable apply(List<?> arguments, SqlNodeList schema);
  }

  /**
   * Calcite table macro created dynamically to squirrel away the
   * schema provided by the EXTEND clause to allow <pre><code>
   * SELECT ... FROM TABLE(fn(arg => value, ...)) (col1 <type1>, ...)
   * </code></pre>
   * This macro wraps the actual input table macro, which does the
   * actual work to build the Druid table. This macro also caches the
   * translated table to avoid the need to recompute the table multiple
   * times.
   */
  protected static class ShimTableMacro implements TableMacro
  {
    private final ExtendedTableMacro delegate;
    private final SqlNodeList schema;
    private TranslatableTable table;

    public ShimTableMacro(ExtendedTableMacro delegate, SqlNodeList schema)
    {
      this.delegate = delegate;
      this.schema = schema;
    }

    @Override
    public TranslatableTable apply(List<?> arguments)
    {
      if (table == null) {
        table = delegate.apply(arguments, schema);
      }
      return table;
    }

    @Override
    public List<FunctionParameter> getParameters()
    {
      return delegate.getParameters();
    }
  }
}
