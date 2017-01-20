/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.planner;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Chars;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.sql.calcite.rel.DruidConvention;
import io.druid.sql.calcite.rel.DruidRel;
import io.druid.sql.calcite.schema.DruidSchema;
import io.druid.sql.calcite.schema.InformationSchema;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.ConversionUtil;
import org.apache.calcite.util.Pair;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Entry points for Calcite.
 */
public class Calcites
{
  private static final Charset DEFAULT_CHARSET = Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME);

  private Calcites()
  {
    // No instantiation.
  }

  public static void setSystemProperties()
  {
    // These properties control the charsets used for SQL literals. I don't see a way to change this except through
    // system properties, so we'll have to set those...

    final String charset = ConversionUtil.NATIVE_UTF16_CHARSET_NAME;
    System.setProperty("saffron.default.charset", Calcites.defaultCharset().name());
    System.setProperty("saffron.default.nationalcharset", Calcites.defaultCharset().name());
    System.setProperty("saffron.default.collation.name", String.format("%s$en_US", charset));
  }

  public static Charset defaultCharset()
  {
    return DEFAULT_CHARSET;
  }

  public static SchemaPlus createRootSchema(final Schema druidSchema)
  {
    final SchemaPlus rootSchema = CalciteSchema.createRootSchema(false, false).plus();
    rootSchema.add(DruidSchema.NAME, druidSchema);
    rootSchema.add(InformationSchema.NAME, new InformationSchema(rootSchema));
    return rootSchema;
  }

  public static String escapeStringLiteral(final String s)
  {
    if (s == null) {
      return "''";
    } else {
      boolean isPlainAscii = true;
      final StringBuilder builder = new StringBuilder("'");
      for (int i = 0; i < s.length(); i++) {
        final char c = s.charAt(i);
        if (Character.isLetterOrDigit(c) || c == ' ') {
          builder.append(c);
          if (c > 127) {
            isPlainAscii = false;
          }
        } else {
          builder.append("\\").append(BaseEncoding.base16().encode(Chars.toByteArray(c)));
          isPlainAscii = false;
        }
      }
      builder.append("'");
      return isPlainAscii ? builder.toString() : "U&" + builder.toString();
    }
  }

  public static PlannerResult plan(
      final Planner planner,
      final String sql
  ) throws SqlParseException, ValidationException, RelConversionException
  {
    SqlExplain explain = null;
    SqlNode parsed = planner.parse(sql);
    if (parsed.getKind() == SqlKind.EXPLAIN) {
      explain = (SqlExplain) parsed;
      parsed = explain.getExplicandum();
    }
    final SqlNode validated = planner.validate(parsed);
    final RelRoot root = planner.rel(validated);

    try {
      return planWithDruidConvention(planner, explain, root);
    }
    catch (RelOptPlanner.CannotPlanException e) {
      // Try again with BINDABLE convention. Used for querying Values, metadata tables, and fallback.
      try {
        return planWithBindableConvention(planner, explain, root);
      }
      catch (Exception e2) {
        e.addSuppressed(e2);
        throw e;
      }
    }
  }

  private static PlannerResult planWithDruidConvention(
      final Planner planner,
      final SqlExplain explain,
      final RelRoot root
  ) throws RelConversionException
  {
    final DruidRel<?> druidRel = (DruidRel<?>) planner.transform(
        Rules.DRUID_CONVENTION_RULES,
        planner.getEmptyTraitSet()
               .replace(DruidConvention.instance())
               .plus(root.collation),
        root.rel
    );

    if (explain != null) {
      return planExplanation(druidRel, explain);
    } else {
      final Supplier<Sequence<Object[]>> resultsSupplier = new Supplier<Sequence<Object[]>>()
      {
        @Override
        public Sequence<Object[]> get()
        {
          if (root.isRefTrivial()) {
            return druidRel.runQuery();
          } else {
            // Add a mapping on top to accommodate root.fields.
            return Sequences.map(
                druidRel.runQuery(),
                new Function<Object[], Object[]>()
                {
                  @Override
                  public Object[] apply(final Object[] input)
                  {
                    final Object[] retVal = new Object[root.fields.size()];
                    for (int i = 0; i < root.fields.size(); i++) {
                      retVal[i] = input[root.fields.get(i).getKey()];
                    }
                    return retVal;
                  }
                }
            );
          }
        }
      };
      return new PlannerResult(resultsSupplier, root.validatedRowType);
    }
  }

  private static PlannerResult planWithBindableConvention(
      final Planner planner,
      final SqlExplain explain,
      final RelRoot root
  ) throws RelConversionException
  {
    BindableRel bindableRel = (BindableRel) planner.transform(
        Rules.BINDABLE_CONVENTION_RULES,
        planner.getEmptyTraitSet()
               .replace(BindableConvention.INSTANCE)
               .plus(root.collation),
        root.rel
    );

    if (!root.isRefTrivial()) {
      // Add a projection on top to accommodate root.fields.
      final List<RexNode> projects = new ArrayList<>();
      final RexBuilder rexBuilder = bindableRel.getCluster().getRexBuilder();
      for (int field : Pair.left(root.fields)) {
        projects.add(rexBuilder.makeInputRef(bindableRel, field));
      }
      bindableRel = new Bindables.BindableProject(
          bindableRel.getCluster(),
          bindableRel.getTraitSet(),
          bindableRel,
          projects,
          root.validatedRowType
      );
    }

    if (explain != null) {
      return planExplanation(bindableRel, explain);
    } else {
      final BindableRel theRel = bindableRel;
      final DataContext dataContext = new DataContext()
      {
        @Override
        public SchemaPlus getRootSchema()
        {
          return null;
        }

        @Override
        public JavaTypeFactory getTypeFactory()
        {
          return (JavaTypeFactory) planner.getTypeFactory();
        }

        @Override
        public QueryProvider getQueryProvider()
        {
          return null;
        }

        @Override
        public Object get(final String name)
        {
          return null;
        }
      };
      final Supplier<Sequence<Object[]>> resultsSupplier = new Supplier<Sequence<Object[]>>()
      {
        @Override
        public Sequence<Object[]> get()
        {
          final Enumerable enumerable = theRel.bind(dataContext);
          return Sequences.simple(enumerable);
        }
      };
      return new PlannerResult(resultsSupplier, root.validatedRowType);
    }
  }

  private static PlannerResult planExplanation(
      final RelNode rel,
      final SqlExplain explain
  )
  {
    final String explanation = RelOptUtil.dumpPlan("", rel, explain.getFormat(), explain.getDetailLevel());
    final Supplier<Sequence<Object[]>> resultsSupplier = Suppliers.ofInstance(
        Sequences.simple(ImmutableList.of(new Object[]{explanation})));
    final RelDataTypeFactory typeFactory = rel.getCluster().getTypeFactory();
    return new PlannerResult(
        resultsSupplier,
        typeFactory.createStructType(
            ImmutableList.of(typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            ImmutableList.of("PLAN")
        )
    );
  }
}
