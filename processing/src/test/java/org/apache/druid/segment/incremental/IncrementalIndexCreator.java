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

package org.apache.druid.segment.incremental;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.introspect.AnnotatedClassResolver;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.jsontype.SubtypeResolver;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This class handles the incremental-index lifecycle for testing.
 * Any index created using this class during the test will be closed automatically once this class is closed.
 *
 * To allow testing multiple incremental-index implementations, this class can be instantiated with any
 * {@code AppendableIndexSpec} instance.
 * Alternatively, this class can instantiate an {@code AppendableIndexSpec} for you given the appendable-index type as
 * a string.
 * This allows tests' parameterization with the appendable-index types as strings.
 *
 * To further facilitate the tests' parameterization, this class supports listing all the available incremental-index
 * implementations, and produce a cartesian product of many parameter options together with each incremental-index
 * implementation.
 */
public class IncrementalIndexCreator implements Closeable
{
  public static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  /**
   * Allows adding support for testing unregistered indexes.
   * It is used by Druid's extensions for the incremental-index.
   *
   * @param c    an index spec class
   * @param name an index spec name
   */
  public static void addIndexSpec(Class<?> c, String name)
  {
    JSON_MAPPER.registerSubtypes(new NamedType(c, name));
  }

  /**
   * Fetch all the available incremental-index implementations.
   * It can be used to parametrize the test. If more parameters are needed, use indexTypeCartesianProduct().
   * @see #indexTypeCartesianProduct(Collection[]).
   *
   * @return a list of all the incremental-index implementations types (String)
   */
  public static List<String> getAppendableIndexTypes()
  {
    SubtypeResolver resolver = JSON_MAPPER.getSubtypeResolver();
    MapperConfig<?> config = JSON_MAPPER.getDeserializationConfig();
    AnnotatedClass cls = AnnotatedClassResolver.resolveWithoutSuperTypes(config, AppendableIndexSpec.class);
    Collection<NamedType> types = resolver.collectAndResolveSubtypesByClass(config, cls);
    return types.stream().map(NamedType::getName).filter(Objects::nonNull).distinct().collect(Collectors.toList());
  }

  public interface IndexCreator
  {
    /**
     * Build an index given a builder and args.
     *
     * @param builder an incremental index builder supplied by the framework
     * @param args a list of arguments that are used to configure the builder
     * @return a new instance of an incremental-index
     */
    IncrementalIndex createIndex(AppendableIndexBuilder builder, Object... args);
  }

  private final Closer closer = Closer.create();

  private final AppendableIndexSpec appendableIndexSpec;

  private final IndexCreator indexCreator;

  /**
   * Initialize the creator.
   *
   * @param spec a spec that can generate a incremental-index builder
   * @param indexCreator a function that generate an index given a builder and arguments
   */
  public IncrementalIndexCreator(AppendableIndexSpec spec, IndexCreator indexCreator)
  {
    this.appendableIndexSpec = spec;
    this.indexCreator = indexCreator;
  }

  /**
   * Initialize the creator.
   *
   * @param indexType an index type (name)
   * @param indexCreator a function that generate an index given a builder and arguments
   */
  public IncrementalIndexCreator(String indexType, IndexCreator indexCreator) throws JsonProcessingException
  {
    this(parseIndexType(indexType), indexCreator);
  }

  /**
   * Generate an AppendableIndexSpec from index type.
   *
   * @param indexType an index type
   * @return AppendableIndexSpec instance of this type
   * @throws JsonProcessingException if failed to to parse the index
   */
  public static AppendableIndexSpec parseIndexType(String indexType) throws JsonProcessingException
  {
    return JSON_MAPPER.readValue(
        StringUtils.format("{\"type\": \"%s\"}", indexType),
        AppendableIndexSpec.class
    );
  }

  /**
   * Create an index given the input args.
   *
   * @param args The arguments for the index-generator
   * @return An incremental-index instance
   */
  public final IncrementalIndex createIndex(Object... args)
  {
    return createIndex(indexCreator, args);
  }

  /**
   * Create an index given the input args with a specialized index-creator.
   *
   * @param args The arguments for the index-generator
   * @return An incremental-index instance
   */
  public final IncrementalIndex createIndex(IndexCreator indexCreator, Object... args)
  {
    return closer.register(indexCreator.createIndex(appendableIndexSpec.builder(), args));
  }

  @Override
  public void close() throws IOException
  {
    closer.close();

    if (appendableIndexSpec instanceof Closeable) {
      ((Closeable) appendableIndexSpec).close();
    }
  }

  /**
   * Generates all the permutations of the parameters with each of the registered appendable index types.
   * It is used to parameterize the tests with all the permutations of the parameters
   * together with all the appnedbale index types.
   *
   * For example, for a parameterized test with the following constrctor:
   * {@code
   *   public IncrementalIndexTest(String indexType, String mode)
   *   {
   *     ...
   *   }
   * }
   *
   * we can test all the input combinations as follows:
   * {@code
   *   @Parameterized.Parameters(name = "{index}: {0}, {1}")
   *   public static Collection<?> constructorFeeder()
   *   {
   *     return IncrementalIndexCreator.indexTypeCartesianProduct(
   *         ImmutableList.of("rollup", "plain")
   *     );
   *   }
   * }
   *
   * @param c a list of collections of parameters
   * @return the cartesian product of all parameters and appendable index types
   */
  public static List<Object[]> indexTypeCartesianProduct(Collection<?>... c)
  {
    Collection<?>[] args = new Collection<?>[c.length + 1];
    args[0] = getAppendableIndexTypes();
    System.arraycopy(c, 0, args, 1, c.length);
    return cartesianProduct(args);
  }

  /**
   * Generates all the permutations of the parameters.
   *
   * @param c a list of collections of parameters
   * @return the cartesian product of all parameters
   */
  private static List<Object[]> cartesianProduct(Collection<?>... c)
  {
    final ArrayList<Object[]> res = new ArrayList<>();
    final int curLength = c.length;

    if (curLength == 0) {
      res.add(new Object[0]);
      return res;
    }

    final int curItem = curLength - 1;
    for (Object[] objList : cartesianProduct(Arrays.copyOfRange(c, 0, curItem))) {
      for (Object o : c[curItem]) {
        Object[] newObjList = Arrays.copyOf(objList, curLength);
        newObjList[curItem] = o;
        res.add(newObjList);
      }
    }

    return res;
  }
}
