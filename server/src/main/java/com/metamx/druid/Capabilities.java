package com.metamx.druid;

/**
 */
public class Capabilities
{
  private final boolean dimensionValuesSorted;

  public static CapabilitiesBuilder builder()
  {
    return new CapabilitiesBuilder();
  }

  private Capabilities(
      boolean dimensionValuesSorted
  )
  {
    this.dimensionValuesSorted = dimensionValuesSorted;
  }

  public boolean dimensionValuesSorted()
  {
    return dimensionValuesSorted;
  }

  public static class CapabilitiesBuilder
  {
    private boolean dimensionValuesSorted = false;

    private CapabilitiesBuilder() {}

    public CapabilitiesBuilder dimensionValuesSorted(boolean value)
    {
      dimensionValuesSorted = value;
      return this;
    }

    public Capabilities build()
    {
      return new Capabilities(
          dimensionValuesSorted
      );
    }
  }
}
