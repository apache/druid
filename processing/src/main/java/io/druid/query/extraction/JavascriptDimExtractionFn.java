package io.druid.query.extraction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ContextFactory;
import org.mozilla.javascript.ScriptableObject;

import java.nio.ByteBuffer;

public class JavascriptDimExtractionFn implements DimExtractionFn
{
  private static Function<String, String> compile(String function) {
    final ContextFactory contextFactory = ContextFactory.getGlobal();
    final Context context = contextFactory.enterContext();
    context.setOptimizationLevel(9);

    final ScriptableObject scope = context.initStandardObjects();

    final org.mozilla.javascript.Function fn = context.compileFunction(scope, function, "fn", 1, null);
    Context.exit();


    return new Function<String, String>()
    {
      public String apply(String input)
      {
        // ideally we need a close() function to discard the context once it is not used anymore
        Context cx = Context.getCurrentContext();
        if (cx == null) {
          cx = contextFactory.enterContext();
        }

        return Context.toString(fn.call(cx, scope, scope, new String[]{input}));
      }
    };
  }

  private static final byte CACHE_TYPE_ID = 0x4;

  private final String function;
  private final Function<String, String> fn;

  @JsonCreator
  public JavascriptDimExtractionFn(
      @JsonProperty("function") String function
  )
  {
    this.function = function;
    this.fn = compile(function);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] bytes = function.getBytes();
    return ByteBuffer.allocate(1 + bytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(bytes)
                     .array();
  }

  @Override
  public String apply(String dimValue)
  {
    return fn.apply(dimValue);
  }
}
