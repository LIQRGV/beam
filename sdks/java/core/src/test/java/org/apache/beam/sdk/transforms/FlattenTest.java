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

import static org.apache.beam.sdk.TestUtils.LINES;
import static org.apache.beam.sdk.TestUtils.LINES2;
import static org.apache.beam.sdk.TestUtils.LINES_ARRAY;
import static org.apache.beam.sdk.TestUtils.NO_LINES;
import static org.apache.beam.sdk.TestUtils.NO_LINES_ARRAY;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CollectionCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.SetCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;

import com.google.common.collect.ImmutableSet;

import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Tests for Flatten.
 */
@RunWith(JUnit4.class)
public class FlattenTest implements Serializable {

  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  private static class ClassWithoutCoder { }


  @Test
  @Category(RunnableOnService.class)
  public void testFlattenPCollectionList() {
    Pipeline p = TestPipeline.create();

    List<List<String>> inputs = Arrays.asList(
      LINES, NO_LINES, LINES2, NO_LINES, LINES, NO_LINES);

    PCollection<String> output =
        makePCollectionListOfStrings(p, inputs)
        .apply(Flatten.<String>pCollections());

    PAssert.that(output).containsInAnyOrder(flattenLists(inputs));
    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testFlattenPCollectionListThenParDo() {
    Pipeline p = TestPipeline.create();

    List<List<String>> inputs = Arrays.asList(
      LINES, NO_LINES, LINES2, NO_LINES, LINES, NO_LINES);

    PCollection<String> output =
        makePCollectionListOfStrings(p, inputs)
        .apply(Flatten.<String>pCollections())
        .apply(ParDo.of(new IdentityFn<String>()));

    PAssert.that(output).containsInAnyOrder(flattenLists(inputs));
    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testFlattenPCollectionListEmpty() {
    Pipeline p = TestPipeline.create();

    PCollection<String> output =
        PCollectionList.<String>empty(p)
        .apply(Flatten.<String>pCollections()).setCoder(StringUtf8Coder.of());

    PAssert.that(output).empty();
    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testEmptyFlattenAsSideInput() {
    Pipeline p = TestPipeline.create();

    final PCollectionView<Iterable<String>> view =
        PCollectionList.<String>empty(p)
        .apply(Flatten.<String>pCollections()).setCoder(StringUtf8Coder.of())
        .apply(View.<String>asIterable());

    PCollection<String> output = p
        .apply(Create.of((Void) null).withCoder(VoidCoder.of()))
        .apply(ParDo.withSideInputs(view).of(new DoFn<Void, String>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    for (String side : c.sideInput(view)) {
                      c.output(side);
                    }
                  }
                }));

    PAssert.that(output).empty();
    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testFlattenPCollectionListEmptyThenParDo() {

    Pipeline p = TestPipeline.create();

    PCollection<String> output =
        PCollectionList.<String>empty(p)
        .apply(Flatten.<String>pCollections()).setCoder(StringUtf8Coder.of())
        .apply(ParDo.of(new IdentityFn<String>()));

    PAssert.that(output).empty();
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testFlattenNoListsNoCoder() {
    // not RunnableOnService because it should fail at pipeline construction time anyhow.
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("cannot provide a Coder for empty");

    Pipeline p = TestPipeline.create();

    PCollectionList.<ClassWithoutCoder>empty(p)
        .apply(Flatten.<ClassWithoutCoder>pCollections());

    p.run();
  }

  /////////////////////////////////////////////////////////////////////////////

  @Test
  @Category(RunnableOnService.class)
  public void testFlattenIterables() {
    Pipeline p = TestPipeline.create();

    PCollection<Iterable<String>> input = p
        .apply(Create.<Iterable<String>>of(LINES)
            .withCoder(IterableCoder.of(StringUtf8Coder.of())));

    PCollection<String> output =
        input.apply(Flatten.<String>iterables());

    PAssert.that(output)
        .containsInAnyOrder(LINES_ARRAY);

    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testFlattenIterablesLists() {
    Pipeline p = TestPipeline.create();

    PCollection<List<String>> input =
        p.apply(Create.<List<String>>of(LINES).withCoder(ListCoder.of(StringUtf8Coder.of())));

    PCollection<String> output = input.apply(Flatten.<String>iterables());

    PAssert.that(output).containsInAnyOrder(LINES_ARRAY);

    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testFlattenIterablesSets() {
    Pipeline p = TestPipeline.create();

    Set<String> linesSet = ImmutableSet.copyOf(LINES);

    PCollection<Set<String>> input =
        p.apply(Create.<Set<String>>of(linesSet).withCoder(SetCoder.of(StringUtf8Coder.of())));

    PCollection<String> output = input.apply(Flatten.<String>iterables());

    PAssert.that(output).containsInAnyOrder(LINES_ARRAY);

    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testFlattenIterablesCollections() {

    Pipeline p = TestPipeline.create();

    Set<String> linesSet = ImmutableSet.copyOf(LINES);

    PCollection<Collection<String>> input =
        p.apply(Create.<Collection<String>>of(linesSet)
                      .withCoder(CollectionCoder.of(StringUtf8Coder.of())));

    PCollection<String> output = input.apply(Flatten.<String>iterables());

    PAssert.that(output).containsInAnyOrder(LINES_ARRAY);

    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testFlattenIterablesEmpty() {
    Pipeline p = TestPipeline.create();

    PCollection<Iterable<String>> input = p
        .apply(Create.<Iterable<String>>of(NO_LINES)
            .withCoder(IterableCoder.of(StringUtf8Coder.of())));

    PCollection<String> output =
        input.apply(Flatten.<String>iterables());

    PAssert.that(output)
        .containsInAnyOrder(NO_LINES_ARRAY);

    p.run();
  }

  /////////////////////////////////////////////////////////////////////////////

  @Test
  @Category(NeedsRunner.class)
  public void testEqualWindowFnPropagation() {
    Pipeline p = TestPipeline.create();

    PCollection<String> input1 =
        p.apply("CreateInput1", Create.of("Input1"))
        .apply("Window1", Window.<String>into(FixedWindows.of(Duration.standardMinutes(1))));
    PCollection<String> input2 =
        p.apply("CreateInput2", Create.of("Input2"))
        .apply("Window2", Window.<String>into(FixedWindows.of(Duration.standardMinutes(1))));

    PCollection<String> output =
        PCollectionList.of(input1).and(input2)
        .apply(Flatten.<String>pCollections());

    p.run();

    Assert.assertTrue(output.getWindowingStrategy().getWindowFn().isCompatible(
        FixedWindows.of(Duration.standardMinutes(1))));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testCompatibleWindowFnPropagation() {
    Pipeline p = TestPipeline.create();

    PCollection<String> input1 =
        p.apply("CreateInput1", Create.of("Input1"))
        .apply("Window1",
            Window.<String>into(Sessions.withGapDuration(Duration.standardMinutes(1))));
    PCollection<String> input2 =
        p.apply("CreateInput2", Create.of("Input2"))
        .apply("Window2",
            Window.<String>into(Sessions.withGapDuration(Duration.standardMinutes(2))));

    PCollection<String> output =
        PCollectionList.of(input1).and(input2)
        .apply(Flatten.<String>pCollections());

    p.run();

    Assert.assertTrue(output.getWindowingStrategy().getWindowFn().isCompatible(
        Sessions.withGapDuration(Duration.standardMinutes(2))));
  }

  @Test
  public void testIncompatibleWindowFnPropagationFailure() {
    Pipeline p = TestPipeline.create();

    PCollection<String> input1 =
        p.apply("CreateInput1", Create.of("Input1"))
        .apply("Window1", Window.<String>into(FixedWindows.of(Duration.standardMinutes(1))));
    PCollection<String> input2 =
        p.apply("CreateInput2", Create.of("Input2"))
        .apply("Window2", Window.<String>into(FixedWindows.of(Duration.standardMinutes(2))));

    try {
      PCollectionList.of(input1).and(input2)
          .apply(Flatten.<String>pCollections());
      Assert.fail("Exception should have been thrown");
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().startsWith(
          "Inputs to Flatten had incompatible window windowFns"));
    }
  }

  @Test
  public void testFlattenGetName() {
    Assert.assertEquals("Flatten.FlattenIterables", Flatten.<String>iterables().getName());
    Assert.assertEquals("Flatten.FlattenPCollectionList", Flatten.<String>pCollections().getName());
  }

  /////////////////////////////////////////////////////////////////////////////

  private static class IdentityFn<T> extends DoFn<T, T> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element());
    }
  }

  private PCollectionList<String> makePCollectionListOfStrings(
      Pipeline p,
      List<List<String>> lists) {
    return makePCollectionList(p, StringUtf8Coder.of(), lists);
  }

  private <T> PCollectionList<T> makePCollectionList(
      Pipeline p,
      Coder<T> coder,
      List<List<T>> lists) {
    List<PCollection<T>> pcs = new ArrayList<>();
    int index = 0;
    for (List<T> list : lists) {
      PCollection<T> pc = p.apply("Create" + (index++), Create.of(list).withCoder(coder));
      pcs.add(pc);
    }
    return PCollectionList.of(pcs);
  }

  private <T> List<T> flattenLists(List<List<T>> lists) {
    List<T> flattened = new ArrayList<>();
    for (List<T> list : lists) {
      flattened.addAll(list);
    }
    return flattened;
  }
}
