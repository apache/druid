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
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.NamedFunction;
import org.apache.druid.segment.nested.NestedDataComplexTypeSerde;
import org.apache.druid.segment.nested.NestedPathFinder;
import org.apache.druid.segment.nested.NestedPathPart;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.segment.nested.StructuredDataProcessor;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class NestedDataExpressions
{
  public static final ExpressionType TYPE = Preconditions.checkNotNull(
      ExpressionType.fromColumnType(NestedDataComplexTypeSerde.TYPE)
  );

  public static class JsonObjectExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "json_object";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      if (args.size() % 2 != 0) {
        throw validationFailed("must have an even number of arguments");
      }

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

            if (!field.type().is(ExprType.STRING)) {
              throw JsonObjectExprMacro.this.validationFailed("field name must be a STRING");
            }
            theMap.put(field.asString(), unwrap(value));
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

  public static class ToJsonStringExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "to_json_string";

    private final ObjectMapper jsonMapper;

    @Inject
    public ToJsonStringExprMacro(
        @Json ObjectMapper jsonMapper
    )
    {
      this.jsonMapper = jsonMapper;
    }

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      final class ToJsonStringExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
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
            final Object unwrapped = unwrap(input);
            final String stringify = unwrapped == null ? null : jsonMapper.writeValueAsString(unwrapped);
            return ExprEval.ofType(
                ExpressionType.STRING,
                stringify
            );
          }
          catch (JsonProcessingException e) {
            throw ToJsonStringExprMacro.this.processingFailed(
                e,
                "unable to stringify [%s] to JSON",
                input.value()
            );
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

    private final ObjectMapper jsonMapper;

    @Inject
    public ParseJsonExprMacro(
        @Json ObjectMapper jsonMapper
    )
    {
      this.jsonMapper = jsonMapper;
    }

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      final class ParseJsonExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        public ParseJsonExpr(List<Expr> args)
        {
          super(name(), args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          ExprEval arg = args.get(0).eval(bindings);
          if (arg.value() == null) {
            return ExprEval.ofComplex(TYPE, null);
          }
          if (arg.type().is(ExprType.STRING)) {
            try {
              return ExprEval.ofComplex(
                  TYPE,
                  jsonMapper.readValue(arg.asString(), Object.class)
              );
            }
            catch (JsonProcessingException e) {
              throw ParseJsonExprMacro.this.processingFailed(e, "bad string input [%s]", arg.asString());
            }
          }
          throw ParseJsonExprMacro.this.validationFailed(
              "invalid input expected %s but got %s instead",
              ExpressionType.STRING,
              arg.type()
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

  public static class TryParseJsonExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "try_parse_json";

    private final ObjectMapper jsonMapper;

    @Inject
    public TryParseJsonExprMacro(
        @Json ObjectMapper jsonMapper
    )
    {
      this.jsonMapper = jsonMapper;
    }

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      final class ParseJsonExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        public ParseJsonExpr(List<Expr> args)
        {
          super(name(), args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          ExprEval arg = args.get(0).eval(bindings);
          if (arg.type().is(ExprType.STRING) && arg.value() != null) {
            try {
              return ExprEval.ofComplex(
                  TYPE,
                  jsonMapper.readValue(arg.asString(), Object.class)
              );
            }
            catch (JsonProcessingException e) {
              return ExprEval.ofComplex(
                  TYPE,
                  null
              );
            }
          }
          return ExprEval.ofComplex(
              TYPE,
              null
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
      final List<NestedPathPart> parts = getJsonPathPartsFromLiteral(this, args.get(1));
      if (args.size() == 3 && args.get(2).isLiteral()) {
        final ExpressionType castTo = ExpressionType.fromString((String) args.get(2).getLiteralValue());
        if (castTo == null) {
          throw JsonValueExprMacro.this.validationFailed(
              "invalid output type: [%s]",
              args.get(2).getLiteralValue()
          );
        }
        final class JsonValueCastExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
        {
          public JsonValueCastExpr(List<Expr> args)
          {
            super(name(), args);
          }

          @Override
          public ExprEval eval(ObjectBinding bindings)
          {
            ExprEval input = args.get(0).eval(bindings);
            return ExprEval.bestEffortOf(
                NestedPathFinder.findLiteral(unwrap(input), parts)
            ).castTo(castTo);
          }

          @Override
          public Expr visit(Shuttle shuttle)
          {
            List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());
            return shuttle.visit(new JsonValueCastExpr(newArgs));
          }

          @Nullable
          @Override
          public ExpressionType getOutputType(InputBindingInspector inspector)
          {
            return castTo;
          }
        }
        return new JsonValueCastExpr(args);
      } else {
        final class JsonValueExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
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
                NestedPathFinder.findLiteral(unwrap(input), parts)
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
            // we cannot infer output type because there could be anything at the path, and, we lack a proper VARIANT type
            return null;
          }
        }
        return new JsonValueExpr(args);
      }
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
      final List<NestedPathPart> parts = getJsonPathPartsFromLiteral(this, args.get(1));
      final class JsonQueryExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
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
              NestedPathFinder.find(unwrap(input), parts)
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

      final class JsonPathsExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
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
          StructuredDataProcessor.ProcessResults info = processor.processFields(unwrap(input));
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

  public static class JsonKeysExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "json_keys";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      final List<NestedPathPart> parts = getJsonPathPartsFromLiteral(this, args.get(1));
      final class JsonKeysExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        public JsonKeysExpr(List<Expr> args)
        {
          super(name(), args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          ExprEval input = args.get(0).eval(bindings);
          return ExprEval.ofType(
              ExpressionType.STRING_ARRAY,
              NestedPathFinder.findKeys(unwrap(input), parts)
          );
        }


        @Override
        public Expr visit(Shuttle shuttle)
        {
          List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());
          return shuttle.visit(new JsonKeysExpr(newArgs));
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return ExpressionType.STRING_ARRAY;
        }
      }
      return new JsonKeysExpr(args);
    }
  }

  @Nullable
  static Object unwrap(ExprEval input)
  {
    return unwrap(input.value());
  }

  static Object unwrap(Object input)
  {
    if (input instanceof Object[]) {
      return Arrays.stream((Object[]) input).map(NestedDataExpressions::unwrap).toArray();
    }
    return StructuredData.unwrap(input);
  }


  static List<NestedPathPart> getJsonPathPartsFromLiteral(NamedFunction fn, Expr arg)
  {
    if (!(arg.isLiteral() && arg.getLiteralValue() instanceof String)) {
      throw fn.validationFailed(
          "second argument [%s] must be a literal [%s] value",
          arg.stringify(),
          ExpressionType.STRING
      );
    }
    final List<NestedPathPart> parts = NestedPathFinder.parseJsonPath(
        (String) arg.getLiteralValue()
    );
    return parts;
  }
}
