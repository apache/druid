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

package org.apache.druid.jackson;

import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.impl.StdTypeResolverBuilder;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import com.fasterxml.jackson.databind.jsontype.impl.TypeNameIdResolver;

import java.util.Collection;

/**
 * A strict {@link TypeIdResolver} implementation that validates all incoming type ids.
 * <p>
 * During deserialization, the type discriminator in the JSON must correspond to a registered subtype;
 * otherwise this resolver will throw an exception instead of silently accepting or defaulting.
 * <p>
 * An optional default implementation may still be configured and used only when the type id is absent.
 */
public class StrictTypeIdResolver extends TypeIdResolverBase
{
  public static class Builder extends StdTypeResolverBuilder
  {
    @Override
    protected TypeIdResolver idResolver(
        MapperConfig<?> config,
        JavaType baseType,
        PolymorphicTypeValidator subtypeValidator,
        Collection<NamedType> subtypes,
        boolean forSer,
        boolean forDeser
    )
    {
      this._customIdResolver = new StrictTypeIdResolver(config, baseType, subtypes, forSer, forDeser);
      return this._customIdResolver;
    }
  }

  protected final JavaType baseType;
  protected final TypeNameIdResolver delegate;

  StrictTypeIdResolver()
  {
    // Required default constructor for Jackson, the instance is never used
    baseType = null;
    delegate = null;
  }

  StrictTypeIdResolver(
      MapperConfig<?> config,
      JavaType baseType,
      Collection<NamedType> subtypes,
      boolean forSer,
      boolean forDeser
  )
  {
    this.baseType = baseType;
    this.delegate = TypeNameIdResolver.construct(config, baseType, subtypes, forSer, forDeser);
  }

  @Override
  public JavaType typeFromId(DatabindContext context, String id) throws JsonProcessingException
  {
    JavaType type = delegate.typeFromId(context, id);
    if (type == null) {
      // in TypeNameIdResolver, it'd fall back to defaultImpl if configured, but we want to error out instead
      throw ((DeserializationContext) context).invalidTypeIdException(
          baseType,
          id,
          "known type ids = " + delegate.getDescForKnownTypeIds()
      );
    }
    return type;
  }

  @Override
  public String idFromValue(Object value)
  {
    return delegate.idFromValue(value);
  }

  @Override
  public String idFromValueAndType(Object value, Class<?> suggestedType)
  {
    return delegate.idFromValueAndType(value, suggestedType);
  }

  @Override
  public String getDescForKnownTypeIds()
  {
    return delegate.getDescForKnownTypeIds();
  }

  @Override
  public Id getMechanism()
  {
    return Id.CUSTOM;
  }
}
