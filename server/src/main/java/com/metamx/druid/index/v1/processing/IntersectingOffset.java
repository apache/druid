package com.metamx.druid.index.v1.processing;

/**
 */
public class IntersectingOffset implements Offset {
  private final Offset lhs;
  private final Offset rhs;

  public IntersectingOffset(
      Offset lhs,
      Offset rhs
  )
  {
    this.lhs = lhs;
    this.rhs = rhs;

    findIntersection();
  }

  @Override
  public int getOffset() {
    return lhs.getOffset();
  }

  @Override
  public void increment() {
    lhs.increment();
    rhs.increment();

    findIntersection();
  }

  private void findIntersection()
  {
    if (! (lhs.withinBounds() && rhs.withinBounds())) {
      return;
    }

    int lhsOffset = lhs.getOffset();
    int rhsOffset = rhs.getOffset();

    while (lhsOffset != rhsOffset) {
      while (lhsOffset < rhsOffset) {
        lhs.increment();
        if (! lhs.withinBounds()) {
          return;
        }

        lhsOffset = lhs.getOffset();
      }

      while (rhsOffset < lhsOffset) {
        rhs.increment();
        if (! rhs.withinBounds()) {
          return;
        }

        rhsOffset = rhs.getOffset();
      }
    }
  }

  @Override
  public boolean withinBounds() {
    return lhs.withinBounds() && rhs.withinBounds();
  }

  @Override
  public Offset clone()
  {
    final Offset lhsClone = lhs.clone();
    final Offset rhsClone = rhs.clone();
    return new IntersectingOffset(lhsClone, rhsClone);
  }
}
