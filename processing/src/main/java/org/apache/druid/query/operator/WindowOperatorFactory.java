package org.apache.druid.query.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.query.operator.window.Processor;

public class WindowOperatorFactory implements OperatorFactory
{
  private Processor processor;

  @JsonCreator
  public WindowOperatorFactory(
      @JsonProperty("processor") Processor processor
  )
  {
    Preconditions.checkNotNull(processor, "processor cannot be null");
    this.processor = processor;
  }

  @JsonProperty("processor")
  public Processor getProcessor()
  {
    return processor;
  }

  @Override
  public Operator wrap(Operator op)
  {
    return new WindowProcessorOperator(processor, op);
  }

  @Override
  public boolean validateEquivalent(OperatorFactory other)
  {
    if (other instanceof WindowOperatorFactory) {
      return processor.validateEquivalent(((WindowOperatorFactory) other).getProcessor());
    }
    return false;
  }

  @Override
  public String toString()
  {
    return "WindowOperatorFactory{" +
           "processor=" + processor +
           '}';
  }
}
