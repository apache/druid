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
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.ExpressionTypeFactory;
import org.apache.druid.math.expr.NamedFunction;
import org.apache.druid.segment.nested.NestedPathFinder;
import org.apache.druid.segment.nested.NestedPathPart;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.segment.nested.StructuredDataProcessor;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class NestedDataExpressions
{
  private static ExpressionType JSON_ARRAY = ExpressionTypeFactory.getInstance().ofArray(ExpressionType.NESTED_DATA);

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
          super(JsonObjectExprMacro.this, args);
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

          return ExprEval.ofComplex(ExpressionType.NESTED_DATA, theMap);
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return ExpressionType.NESTED_DATA;
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
          super(ToJsonStringExprMacro.this, args);
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
          super(ParseJsonExprMacro.this, args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          ExprEval arg = args.get(0).eval(bindings);
          if (arg.value() == null) {
            return ExprEval.ofComplex(ExpressionType.NESTED_DATA, null);
          }
          if (arg.type().is(ExprType.STRING)) {
            try {
              return ExprEval.ofComplex(
                  ExpressionType.NESTED_DATA,
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

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return ExpressionType.NESTED_DATA;
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
          super(TryParseJsonExprMacro.this, args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          ExprEval arg = args.get(0).eval(bindings);
          if (arg.type().is(ExprType.STRING) && arg.value() != null) {
            try {
              return ExprEval.ofComplex(
                  ExpressionType.NESTED_DATA,
                  jsonMapper.readValue(arg.asString(), Object.class)
              );
            }
            catch (JsonProcessingException e) {
              return ExprEval.ofComplex(
                  ExpressionType.NESTED_DATA,
                  null
              );
            }
          }
          return ExprEval.ofComplex(
              ExpressionType.NESTED_DATA,
              null
          );
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return ExpressionType.NESTED_DATA;
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
      if (args.get(1).isLiteral()) {
        if (args.size() == 3 && args.get(2).isLiteral()) {
          return new JsonValueCastExpr(args);
        } else {
          return new JsonValueExpr(args);
        }
      } else {
        return new JsonValueDynamicExpr(args);
      }
    }

    final class JsonValueExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
    {
      private final List<NestedPathPart> parts;

      public JsonValueExpr(List<Expr> args)
      {
        super(JsonValueExprMacro.this, args);
        this.parts = getJsonPathPartsFromLiteral(JsonValueExprMacro.this, args.get(1));
      }

      @Override
      public ExprEval eval(ObjectBinding bindings)
      {
        final ExprEval input = args.get(0).eval(bindings);
        final ExprEval valAtPath = ExprEval.bestEffortOf(
            NestedPathFinder.find(unwrap(input), parts)
        );
        if (valAtPath.type().isPrimitive() || valAtPath.type().isPrimitiveArray()) {
          return valAtPath;
        }
        return ExprEval.of(null);
      }

      @Nullable
      @Override
      public ExpressionType getOutputType(InputBindingInspector inspector)
      {
        // we cannot infer output type because there could be anything at the path, and, we lack a proper VARIANT type
        return null;
      }
    }

    final class JsonValueCastExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
    {
      private final List<NestedPathPart> parts;
      private final ExpressionType castTo;

      public JsonValueCastExpr(List<Expr> args)
      {
        super(JsonValueExprMacro.this, args);
        this.parts = getJsonPathPartsFromLiteral(JsonValueExprMacro.this, args.get(1));
        this.castTo = ExpressionType.fromString((String) args.get(2).getLiteralValue());
        if (castTo == null) {
          throw JsonValueExprMacro.this.validationFailed(
              "invalid output type: [%s]",
              args.get(2).getLiteralValue()
          );
        }
      }

      @Override
      public ExprEval eval(ObjectBinding bindings)
      {
        final ExprEval input = args.get(0).eval(bindings);
        final ExprEval valAtPath = ExprEval.bestEffortOf(
            NestedPathFinder.find(unwrap(input), parts)
        );
        if (valAtPath.type().isPrimitive() || valAtPath.type().isPrimitiveArray()) {
          return valAtPath.castTo(castTo);
        }
        return ExprEval.ofType(castTo, null);
      }

      @Nullable
      @Override
      public ExpressionType getOutputType(InputBindingInspector inspector)
      {
        return castTo;
      }
    }

    final class JsonValueDynamicExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
    {
      public JsonValueDynamicExpr(List<Expr> args)
      {
        super(JsonValueExprMacro.this, args);
      }

      @Override
      public ExprEval eval(ObjectBinding bindings)
      {
        final ExprEval input = args.get(0).eval(bindings);
        final ExprEval path = args.get(1).eval(bindings);
        final ExpressionType castTo;
        if (args.size() == 3) {
          castTo = ExpressionType.fromString(args.get(2).eval(bindings).asString());
          if (castTo == null) {
            throw JsonValueExprMacro.this.validationFailed(
                "invalid output type: [%s]",
                args.get(2).getLiteralValue()
            );
          }
        } else {
          castTo = null;
        }
        final List<NestedPathPart> parts = NestedPathFinder.parseJsonPath(path.asString());
        final ExprEval<?> valAtPath = ExprEval.bestEffortOf(NestedPathFinder.find(unwrap(input), parts));
        if (valAtPath.type().isPrimitive() || valAtPath.type().isPrimitiveArray()) {
          return castTo == null ? valAtPath : valAtPath.castTo(castTo);
        }
        return castTo == null ? ExprEval.of(null) : ExprEval.ofType(castTo, null);
      }

      @Nullable
      @Override
      public ExpressionType getOutputType(InputBindingInspector inspector)
      {
        // we cannot infer output type because there could be anything at the path, and, we lack a proper VARIANT type
        return null;
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
      if (args.get(1).isLiteral()) {
        return new JsonQueryExpr(args);
      } else {
        return new JsonQueryDynamicExpr(args);
      }
    }

    final class JsonQueryExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
    {
      private final List<NestedPathPart> parts;

      public JsonQueryExpr(List<Expr> args)
      {
        super(JsonQueryExprMacro.this, args);
        this.parts = getJsonPathPartsFromLiteral(JsonQueryExprMacro.this, args.get(1));
      }

      @Override
      public ExprEval eval(ObjectBinding bindings)
      {
        ExprEval input = args.get(0).eval(bindings);
        return ExprEval.ofComplex(
            ExpressionType.NESTED_DATA,
            NestedPathFinder.find(unwrap(input), parts)
        );
      }

      @Nullable
      @Override
      public ExpressionType getOutputType(InputBindingInspector inspector)
      {
        // call all the output JSON typed
        return ExpressionType.NESTED_DATA;
      }
    }

    final class JsonQueryDynamicExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
    {
      public JsonQueryDynamicExpr(List<Expr> args)
      {
        super(JsonQueryExprMacro.this, args);
      }

      @Override
      public ExprEval eval(ObjectBinding bindings)
      {
        ExprEval input = args.get(0).eval(bindings);
        ExprEval path = args.get(1).eval(bindings);
        final List<NestedPathPart> parts = NestedPathFinder.parseJsonPath(path.asString());
        return ExprEval.ofComplex(
            ExpressionType.NESTED_DATA,
            NestedPathFinder.find(unwrap(input), parts)
        );
      }

      @Nullable
      @Override
      public ExpressionType getOutputType(InputBindingInspector inspector)
      {
        // call all the output JSON typed
        return ExpressionType.NESTED_DATA;
      }
    }
  }

  public static class JsonQueryArrayExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "json_query_array";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      if (args.get(1).isLiteral()) {
        return new JsonQueryArrayExpr(args);
      } else {
        return new JsonQueryArrayDynamicExpr(args);
      }
    }

    final class JsonQueryArrayExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
    {
      private final List<NestedPathPart> parts;

      public JsonQueryArrayExpr(List<Expr> args)
      {
        super(JsonQueryArrayExprMacro.this, args);
        this.parts = getJsonPathPartsFromLiteral(JsonQueryArrayExprMacro.this, args.get(1));
      }

      @Override
      public ExprEval eval(ObjectBinding bindings)
      {
        ExprEval input = args.get(0).eval(bindings);
        final Object value = NestedPathFinder.find(unwrap(input), parts);
        if (value instanceof List) {
          return ExprEval.ofArray(
              JSON_ARRAY,
              ExprEval.bestEffortArray((List) value).asArray()
          );
        }
        return ExprEval.ofArray(
            JSON_ARRAY,
            ExprEval.bestEffortOf(value).asArray()
        );
      }

      @Nullable
      @Override
      public ExpressionType getOutputType(InputBindingInspector inspector)
      {
        // call all the output JSON typed
        return ExpressionType.NESTED_DATA;
      }
    }

    final class JsonQueryArrayDynamicExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
    {
      public JsonQueryArrayDynamicExpr(List<Expr> args)
      {
        super(JsonQueryArrayExprMacro.this, args);
      }

      @Override
      public ExprEval eval(ObjectBinding bindings)
      {
        ExprEval input = args.get(0).eval(bindings);
        ExprEval path = args.get(1).eval(bindings);
        final List<NestedPathPart> parts = NestedPathFinder.parseJsonPath(path.asString());
        final Object value = NestedPathFinder.find(unwrap(input), parts);
        if (value instanceof List) {
          return ExprEval.ofArray(
              JSON_ARRAY,
              ExprEval.bestEffortArray((List) value).asArray()
          );
        }
        return ExprEval.ofArray(
            JSON_ARRAY,
            ExprEval.bestEffortOf(value).asArray()
        );
      }

      @Nullable
      @Override
      public ExpressionType getOutputType(InputBindingInspector inspector)
      {
        // call all the output ARRAY<COMPLEX<json>> typed
        return JSON_ARRAY;
      }
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
        public ProcessedValue<?> processField(ArrayList<NestedPathPart> fieldPath, @Nullable Object fieldValue)
        {
          // do nothing, we only want the list of fields returned by this processor
          return ProcessedValue.NULL_LITERAL;
        }

        @Nullable
        @Override
        public ProcessedValue<?> processArrayField(
            ArrayList<NestedPathPart> fieldPath,
            @Nullable List<?> array
        )
        {
          // we only want to return a non-null value here if the value is an array of primitive values
          ExprEval<?> eval = ExprEval.bestEffortArray(array);
          if (eval.type().isPrimitiveArray()) {
            return ProcessedValue.NULL_LITERAL;
          }
          return null;
        }
      };

      final class JsonPathsExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        public JsonPathsExpr(List<Expr> args)
        {
          super(JsonPathsExprMacro.this, args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          ExprEval input = args.get(0).eval(bindings);
          // maybe in the future ProcessResults should deal in PathFinder.PathPart instead of strings for fields
          StructuredDataProcessor.ProcessResults info = processor.processFields(unwrap(input));
          List<String> transformed = info.getLiteralFields()
                                         .stream()
                                         .map(NestedPathFinder::toNormalizedJsonPath)
                                         .collect(Collectors.toList());
          return ExprEval.ofType(
              ExpressionType.STRING_ARRAY,
              transformed
          );
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
          super(JsonKeysExprMacro.this, args);
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
