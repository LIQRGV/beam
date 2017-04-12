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
package org.apache.beam.sdk.io.redis;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test on the Redis IO.
 */
public class RedisIOTest {

  private static final Logger LOG = LoggerFactory.getLogger(RedisIOTest.class);

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testEstimatedSizeBytes() throws Exception {
    final FakeRedisService service = new FakeRedisService();
    service.clear();
    for (int i = 0; i < 10; i++) {
      service.put("Foo " + i, "Bar " + i);
    }

    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
    RedisIO.RedisSource source = new RedisIO.RedisSource("Foo.*",
        new SerializableFunction<PipelineOptions, RedisService>() {
          @Override
          public RedisService apply(PipelineOptions input) {
            return service;
          }
        }, null);
    long estimatedSizeBytes = source.getEstimatedSizeBytes(pipelineOptions);
    LOG.info("Estimated size: {}", estimatedSizeBytes);
    assertEquals(100, estimatedSizeBytes);
  }

  @Test
  public void testGetRead() throws Exception {
    FakeRedisService service = new FakeRedisService();
    for (int i = 0; i < 1000; i++) {
      service.put("Key " + i, "Value " + i);
    }

    PCollection<KV<String, String>> output =
        pipeline.apply(RedisIO.read().withRedisService(service));

    PAssert.thatSingleton(output.apply("Count", Count.<KV<String, String>>globally()))
        .isEqualTo(1000L);
    ArrayList<KV<String, String>> expected = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      KV<String, String> kv = KV.of("Key " + i, "Value " + i);
      expected.add(kv);
    }
    PAssert.that(output).containsInAnyOrder(expected);

    pipeline.run();
  }

  /**
   * A {@link RedisService} implementation that stores key/value pairs in memory.
   */
  private static class FakeRedisService implements RedisService {

    private static final Map<String, String> store = new HashMap<>();

    public void put(String key, String value) {
      store.put(key, value);
    }

    public String get(String key) {
      return store.get(key);
    }

    public void clear() {
      store.clear();
    }

    @Override
    public FakeRedisReader createReader(RedisIO.RedisSource source) {
      return new FakeRedisReader(source);
    }

    static class FakeRedisReader extends BoundedSource.BoundedReader<KV<String, String>> {

      private final RedisIO.RedisSource source;
      private Iterator<String> keysIterator;
      private KV<String, String> current;

      public FakeRedisReader(RedisIO.RedisSource source) {
        this.source = source;
      }

      @Override
      public boolean start() {
        keysIterator = store.keySet().iterator();
        return advance();
      }

      @Override
      public boolean advance() {
        if (keysIterator.hasNext()) {
          String key = keysIterator.next();
          if (source.keyPattern != null && !source.keyPattern.equals("*")) {
            if (!key.matches(source.keyPattern)) {
              return false;
            }
          }
          String value = store.get(key);
          KV<String, String> record = KV.of(key, value);
          current = record;
          return true;
        }
        return false;
      }

      @Override
      public void close() {
        keysIterator = null;
        current = null;
      }

      @Override
      public KV<String, String> getCurrent() {
        if (current == null) {
          throw new NoSuchElementException();
        }
        return current;
      }

      @Override
      public RedisIO.RedisSource getCurrentSource() {
        return  source;
      }

    }

    @Override
    public long getEstimatedSizeBytes() {
      long size = 0L;
      Iterator<String> keysIterator = store.keySet().iterator();
      long count = 0;
      while (keysIterator.hasNext() && count < 10) {
        count++;
        String key = keysIterator.next();
        String value = store.get(key);
        size += key.getBytes().length + value.getBytes().length;
      }
      return size;
    }

    @Override
    public boolean isClusterEnabled() {
      return false;
    }

    @Override
    public List<RedisNode> getClusterNodes() {
      return null;
    }

    @Override
    public int getKeySlot(String key) {
      return 0;
    }

  }

}
