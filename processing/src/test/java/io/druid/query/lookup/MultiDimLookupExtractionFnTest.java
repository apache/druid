package io.druid.query.lookup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.*;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.query.extraction.MultiKeyMapLookupExtractor;
import org.apache.commons.collections.keyvalue.MultiKey;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@RunWith(Parameterized.class)
public class MultiDimLookupExtractionFnTest
{
  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder()
  {
    return Iterables.transform(
        Sets.cartesianProduct(
            ImmutableList.of(
                ImmutableSet.of("", "MISSING VALUE")
            )
        ), new Function<List<?>, Object[]>()
        {
          @Nullable
          @Override
          public Object[] apply(List<?> input)
          {
            return input.toArray();
          }
        }
    );
  }

  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  private final String replaceMissing;

  public MultiDimLookupExtractionFnTest(String replaceMissing)
  {
    this.replaceMissing = replaceMissing;
  }

  @Test
  public void testEqualAndHash()
  {
    final String[] key = new String[] {"foo1", "foo2", "foo3"};
    final MultiDimLookupExtractionFn multiDimLookupExtractionFn1 = new MultiDimLookupExtractionFn(
        new MapLookupExtractor(
            ImmutableMap.<Object, String>of(
                new MultiKey(key), "bar"
            ),
            false
        ),
        replaceMissing,
        false,
        key.length
    );
    final MultiDimLookupExtractionFn multiDimLookupExtractionFn2 = new MultiDimLookupExtractionFn(
        new MapLookupExtractor(
            ImmutableMap.<Object, String>of(
                new MultiKey(key), "bar"
            ),
            false
        ),
        replaceMissing,
        false,
        key.length
    );
    final MultiDimLookupExtractionFn multiDimLookupExtractionFn3 = new MultiDimLookupExtractionFn(
        new MapLookupExtractor(
            ImmutableMap.<Object, String>of(
                new MultiKey(key), "bar2"
            ),
            false
        ),
        replaceMissing,
        false,
        key.length
    );

    Assert.assertEquals(multiDimLookupExtractionFn1, multiDimLookupExtractionFn2);
    Assert.assertEquals(multiDimLookupExtractionFn1.hashCode(), multiDimLookupExtractionFn2.hashCode());
    Assert.assertNotEquals(multiDimLookupExtractionFn1, multiDimLookupExtractionFn3);
    Assert.assertNotEquals(multiDimLookupExtractionFn1.hashCode(), multiDimLookupExtractionFn3.hashCode());
  }

  @Test
  public void testSimpleLookup()
  {
    final String[] key = new String[] {"foo1", "foo2", "foo3"};
    final String expected = "bar";
    final MultiDimLookupExtractionFn multiDimLookupExtractionFn1 = new MultiDimLookupExtractionFn(
        new MapLookupExtractor(
            ImmutableMap.<Object, String>of(
                new MultiKey(key), expected
            ),
            false
        ),
        replaceMissing,
        false,
        key.length
    );

    List<String> keyList = Arrays.asList(key);
    String ret = multiDimLookupExtractionFn1.apply(keyList);

    Assert.assertEquals(expected, ret);

    List<String> badKey = ImmutableList.of("foo1", "foo2", "foo4");
    Assert.assertEquals(Strings.emptyToNull(replaceMissing), multiDimLookupExtractionFn1.apply(badKey));
  }

  @Test
  public void testSimpleSerde() throws IOException
  {
    final List<String> keyValue = Lists.newArrayList("foo1", "foo2", "foo3", "bar");
    final MultiDimLookupExtractionFn multiDimLookupExtractionFn1 = new MultiDimLookupExtractionFn(
        new MultiKeyMapLookupExtractor(
            ImmutableList.of(keyValue),
            false
        ),
        replaceMissing,
        false,
        3
    );
    final String str1 = OBJECT_MAPPER.writeValueAsString(multiDimLookupExtractionFn1);

    final MultiDimLookupExtractionFn multiDimLookupExtractionFn2 =
        OBJECT_MAPPER.readValue(str1, MultiDimLookupExtractionFn.class);

    Assert.assertEquals(Strings.emptyToNull(replaceMissing), multiDimLookupExtractionFn2.getReplaceMissingValueWith());
    Assert.assertArrayEquals(multiDimLookupExtractionFn1.getCacheKey(), multiDimLookupExtractionFn2.getCacheKey());

    Assert.assertEquals(
        str1,
        OBJECT_MAPPER.writeValueAsString(multiDimLookupExtractionFn2)
    );
  }
}
