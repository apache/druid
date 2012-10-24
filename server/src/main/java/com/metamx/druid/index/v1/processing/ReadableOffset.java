package com.metamx.druid.index.v1.processing;

/**
 * A ReadableOffset is an object that provides an integer offset, ostensibly as an index into an array.
 *
 * See the companion class Offset, for more context on how this could be useful.  A ReadableOffset should be
 * given to classes (e.g. FloatMetricSelector objects) by something which keeps a reference to the base Offset object
 * and increments it.
 */
public interface ReadableOffset
{
    int getOffset();
}

