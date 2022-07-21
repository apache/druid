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


package org.apache.druid.query.expression;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.segment.nested.NestedDataComplexTypeSerde;
import org.apache.druid.segment.nested.NestedPathFinder;
import org.apache.druid.segment.nested.NestedPathPart;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.segment.nested.StructuredDataProcessor;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class NestedDataExpressions
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  public static final ExpressionType TYPE = Preconditions.checkNotNull(
      ExpressionType.fromColumnType(NestedDataComplexTypeSerde.TYPE)
  );

  public static class StructExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "struct";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      Preconditions.checkArgument(args.size() % 2 == 0);
      class StructExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        public StructExpr(List<Expr> args)
        {
          super(NAME, args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          HashMap<String, Object> theMap = new HashMap<>();
          for (int i = 0; i < args.size(); i += 2) {
            ExprEval field = args.get(i).eval(bindings);
            ExprEval value = args.get(i + 1).eval(bindings);

            Preconditions.checkArgument(field.type().is(ExprType.STRING), "field name must be a STRING");
            theMap.put(field.asString(), value.value());
          }

          return ExprEval.ofComplex(TYPE, theMap);
        }

        @Override
        public Expr visit(Shuttle shuttle)
        {
          List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());
          return shuttle.visit(new StructExpr(newArgs));
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return TYPE;
        }
      }
      return new StructExpr(args);
    }
  }

  public static class JsonObjectExprMacro extends StructExprMacro
  {
    public static final String NAME = "json_object";
    
    @Override
    public String name()
    {
      return NAME;
    }
  }

  public static class ToJsonExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "to_json";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      class ToJsonExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        public ToJsonExpr(List<Expr> args)
        {
          super(name(), args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          ExprEval input = args.get(0).eval(bindings);
          return ExprEval.ofComplex(
              TYPE,
              maybeUnwrapStructuredData(input)
          );
        }

        @Override
        public Expr visit(Shuttle shuttle)
        {
          List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());
          return shuttle.visit(new ToJsonExpr(newArgs));
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return TYPE;
        }
      }
      return new ToJsonExpr(args);
    }
  }

  public static class ToJsonStringExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "to_json_string";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      class ToJsonStringExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        public ToJsonStringExpr(List<Expr> args)
        {
          super(name(), args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          ExprEval input = args.get(0).eval(bindings);
          try {
            final Object unwrapped = maybeUnwrapStructuredData(input);
            final String stringify = unwrapped == null ? null : JSON_MAPPER.writeValueAsString(unwrapped);
            return ExprEval.ofType(
                ExpressionType.STRING,
                stringify
            );
          }
          catch (JsonProcessingException e) {
            throw new IAE(e, "Unable to stringify [%s] to JSON", input.value());
          }
        }

        @Override
        public Expr visit(Shuttle shuttle)
        {
          List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());
          return shuttle.visit(new ToJsonStringExpr(newArgs));
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return ExpressionType.STRING;
        }
      }
      return new ToJsonStringExpr(args);
    }
  }

  public static class ParseJsonExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "parse_json";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      class ParseJsonExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        public ParseJsonExpr(List<Expr> args)
        {
          super(name(), args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          ExprEval arg = args.get(0).eval(bindings);
          Object parsed = maybeUnwrapStructuredData(arg);
          if (arg.type().is(ExprType.STRING) && arg.value() != null && maybeJson(arg.asString())) {
            try {
              parsed = JSON_MAPPER.readValue(arg.asString(), Object.class);
            }
            catch (JsonProcessingException e) {
              throw new IAE("Bad string input [%s] to [%s]", arg.asString(), name());
            }
          }
          return ExprEval.ofComplex(
              TYPE,
              parsed
          );
        }

        @Override
        public Expr visit(Shuttle shuttle)
        {
          List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());
          return shuttle.visit(new ParseJsonExpr(newArgs));
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return TYPE;
        }
      }
      return new ParseJsonExpr(args);
    }
  }



  public static class GetPathExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "get_path";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      final List<NestedPathPart> parts = getArg1PathPartsFromLiteral(name(), args);
      class GetPathExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        public GetPathExpr(List<Expr> args)
        {
          super(name(), args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          ExprEval input = args.get(0).eval(bindings);
          return ExprEval.bestEffortOf(
              NestedPathFinder.findLiteral(maybeUnwrapStructuredData(input), parts)
          );
        }

        @Override
        public Expr visit(Shuttle shuttle)
        {
          List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());
          return shuttle.visit(new GetPathExpr(newArgs));
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          // we cannot infer the output type (well we could say it is 'STRING' right now because is all we support...
          return null;
        }
      }
      return new GetPathExpr(args);
    }
  }

  public static class JsonQueryExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "json_query";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      final List<NestedPathPart> parts = getArg1JsonPathPartsFromLiteral(name(), args);
      class JsonQueryExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        public JsonQueryExpr(List<Expr> args)
        {
          super(name(), args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          ExprEval input = args.get(0).eval(bindings);
          return ExprEval.ofComplex(
              TYPE,
              NestedPathFinder.find(maybeUnwrapStructuredData(input), parts)
          );
        }

        @Override
        public Expr visit(Shuttle shuttle)
        {
          List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());
          return shuttle.visit(new JsonQueryExpr(newArgs));
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          // call all the output JSON typed
          return TYPE;
        }
      }
      return new JsonQueryExpr(args);
    }
  }

  public static class JsonValueExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "json_value";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      final List<NestedPathPart> parts = getArg1JsonPathPartsFromLiteral(name(), args);
      class JsonValueExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        public JsonValueExpr(List<Expr> args)
        {
          super(name(), args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          ExprEval input = args.get(0).eval(bindings);
          return ExprEval.bestEffortOf(
              NestedPathFinder.findLiteral(maybeUnwrapStructuredData(input), parts)
          );
        }

        @Override
        public Expr visit(Shuttle shuttle)
        {
          List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());
          return shuttle.visit(new JsonValueExpr(newArgs));
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          // we cannot infer the output type (well we could say it is 'STRING' right now because is all we support...
          return null;
        }
      }
      return new JsonValueExpr(args);
    }
  }

  public static class ListPathsExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "list_paths";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      final StructuredDataProcessor processor = new StructuredDataProcessor()
      {
        @Override
        public int processLiteralField(String fieldName, Object fieldValue)
        {
          // do nothing, we only want the list of fields returned by this processor
          return 0;
        }
      };

      class ListPathsExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        public ListPathsExpr(List<Expr> args)
        {
          super(name(), args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          ExprEval input = args.get(0).eval(bindings);
          StructuredDataProcessor.ProcessResults info = processor.processFields(maybeUnwrapStructuredData(input));
          return ExprEval.ofType(
              ExpressionType.STRING_ARRAY,
              ImmutableList.copyOf(info.getLiteralFields())
          );
        }

        @Override
        public Expr visit(Shuttle shuttle)
        {
          List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());
          return shuttle.visit(new ListPathsExpr(newArgs));
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return ExpressionType.STRING_ARRAY;
        }
      }
      return new ListPathsExpr(args);
    }
  }

  public static class JsonPathsExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "json_paths";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      final StructuredDataProcessor processor = new StructuredDataProcessor()
      {
        @Override
        public int processLiteralField(String fieldName, Object fieldValue)
        {
          // do nothing, we only want the list of fields returned by this processor
          return 0;
        }
      };

      class JsonPathsExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        public JsonPathsExpr(List<Expr> args)
        {
          super(name(), args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          ExprEval input = args.get(0).eval(bindings);
          // maybe in the future ProcessResults should deal in PathFinder.PathPart instead of strings for fields
          StructuredDataProcessor.ProcessResults info = processor.processFields(maybeUnwrapStructuredData(input));
          List<String> transformed = info.getLiteralFields()
                                        .stream()
                                        .map(p -> NestedPathFinder.toNormalizedJsonPath(NestedPathFinder.parseJqPath(p)))
                                        .collect(Collectors.toList());
          return ExprEval.ofType(
              ExpressionType.STRING_ARRAY,
              transformed
          );
        }

        @Override
        public Expr visit(Shuttle shuttle)
        {
          List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());
          return shuttle.visit(new JsonPathsExpr(newArgs));
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return ExpressionType.STRING_ARRAY;
        }
      }
      return new JsonPathsExpr(args);
    }
  }

  public static class ListKeysExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "list_keys";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      final List<NestedPathPart> parts = getArg1PathPartsFromLiteral(name(), args);
      class ListKeysExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        public ListKeysExpr(List<Expr> args)
        {
          super(name(), args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          ExprEval input = args.get(0).eval(bindings);
          return ExprEval.ofType(
              ExpressionType.STRING_ARRAY,
              NestedPathFinder.findKeys(maybeUnwrapStructuredData(input), parts)
          );
        }


        @Override
        public Expr visit(Shuttle shuttle)
        {
          List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());
          return shuttle.visit(new ListKeysExpr(newArgs));
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return ExpressionType.STRING_ARRAY;
        }
      }
      return new ListKeysExpr(args);
    }
  }

  public static class JsonKeysExprMacro extends ListKeysExprMacro
  {
    public static final String NAME = "json_keys";

    @Override
    public String name()
    {
      return NAME;
    }
  }

  @Nullable
  static Object maybeUnwrapStructuredData(ExprEval input)
  {
    if (input.value() instanceof StructuredData) {
      StructuredData data = (StructuredData) input.value();
      return data.getValue();
    }
    return input.value();
  }

  static List<NestedPathPart> getArg1PathPartsFromLiteral(String fnName, List<Expr> args)
  {
    if (!(args.get(1).isLiteral() && args.get(1).getLiteralValue() instanceof String)) {
      throw new IAE(
          "Function[%s] second argument [%s] must be a literal [%s] value",
          fnName,
          args.get(1).stringify(),
          ExpressionType.STRING
      );
    }
    final String path = (String) args.get(1).getLiteralValue();
    List<NestedPathPart> parts;
    try {
      parts = NestedPathFinder.parseJsonPath(path);
    }
    catch (IllegalArgumentException iae) {
      parts = NestedPathFinder.parseJqPath(path);
    }
    return parts;
  }

  static List<NestedPathPart> getArg1JsonPathPartsFromLiteral(String fnName, List<Expr> args)
  {
    if (!(args.get(1).isLiteral() && args.get(1).getLiteralValue() instanceof String)) {
      throw new IAE(
          "Function[%s] second argument [%s] must be a literal [%s] value",
          fnName,
          args.get(1).stringify(),
          ExpressionType.STRING
      );
    }
    final List<NestedPathPart> parts = NestedPathFinder.parseJsonPath(
        (String) args.get(1).getLiteralValue()
    );
    return parts;
  }

  static boolean maybeJson(@Nullable String val)
  {
    if (val == null) {
      return false;
    }
    if (val.isEmpty()) {
      return false;
    }
    return val.startsWith("[") || val.startsWith("{") || val.startsWith("\"") || Character.isDigit(val.charAt(0));
  }
}
