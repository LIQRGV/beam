/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.transforms;

import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A {@link SerializableFunction} which is not a <i>functional interface</i>.
 * Concrete subclasses allow us to infer type information, which in turn aids
 * {@link org.apache.beam.sdk.coders.Coder Coder} inference.
 */
public abstract class SimpleFunction<InputT, OutputT>
    implements SerializableFunction<InputT, OutputT>, HasDisplayData {

  public static <InputT, OutputT>
      SimpleFunction<InputT, OutputT> fromSerializableFunctionWithOutputType(
          SerializableFunction<InputT, OutputT> fn, TypeDescriptor<OutputT> outputType) {
    return new SimpleFunctionWithOutputType<>(fn, outputType);
  }

  /**
   * Returns a {@link TypeDescriptor} capturing what is known statically
   * about the input type of this {@code OldDoFn} instance's most-derived
   * class.
   *
   * <p>See {@link #getOutputTypeDescriptor} for more discussion.
   */
  public TypeDescriptor<InputT> getInputTypeDescriptor() {
    return new TypeDescriptor<InputT>(this) {};
  }

  /**
   * Returns a {@link TypeDescriptor} capturing what is known statically
   * about the output type of this {@code OldDoFn} instance's
   * most-derived class.
   *
   * <p>In the normal case of a concrete {@code OldDoFn} subclass with
   * no generic type parameters of its own (including anonymous inner
   * classes), this will be a complete non-generic type, which is good
   * for choosing a default output {@code Coder<OutputT>} for the output
   * {@code PCollection<OutputT>}.
   */
  public TypeDescriptor<OutputT> getOutputTypeDescriptor() {
    return new TypeDescriptor<OutputT>(this) {};
  }

  /**
    * {@inheritDoc}
    *
    * <p>By default, does not register any display data. Implementors may override this method
    * to provide their own display data.
    */
  @Override
  public void populateDisplayData(DisplayData.Builder builder) {}

  /**
   * A {@link SimpleFunction} built from a {@link SerializableFunction}, having
   * a known output type that is explicitly set.
   */
  private static class SimpleFunctionWithOutputType<InputT, OutputT>
      extends SimpleFunction<InputT, OutputT> {

    private final SerializableFunction<InputT, OutputT> fn;
    private final TypeDescriptor<OutputT> outputType;

    public SimpleFunctionWithOutputType(
        SerializableFunction<InputT, OutputT> fn,
        TypeDescriptor<OutputT> outputType) {
      this.fn = fn;
      this.outputType = outputType;
    }


    @Override
    public OutputT apply(InputT input) {
      return fn.apply(input);
    }

    @Override
    public TypeDescriptor<OutputT> getOutputTypeDescriptor() {
      return outputType;
    }
  }
}
