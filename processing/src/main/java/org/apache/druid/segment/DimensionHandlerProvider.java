package org.apache.druid.segment;

public interface DimensionHandlerProvider
    <EncodedType extends Comparable<EncodedType>, EncodedKeyComponentType, ActualType extends Comparable<ActualType>>
{
  DimensionHandler<EncodedType, EncodedKeyComponentType, ActualType> get(String dimensionName);
}
