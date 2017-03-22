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

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.beam.sdk.values.KV;

import redis.clients.jedis.Jedis;

/**
 * An implementation of the {@link RedisService} that actually communicates with a Redis server.
 */
public class RedisServiceImpl implements RedisService {

  private final RedisConnection connection;

  public RedisServiceImpl(RedisConnection connection) {
    this.connection = connection;
  }

  private class RedisReaderImpl implements Reader {

    private final String keyPattern;
    private Jedis jedis;
    private Iterator<String> keysIterator;
    private KV<String, String> current;

    public RedisReaderImpl(String keyPattern) {
      this.keyPattern = keyPattern;
    }

    @Override
    public boolean start() {
      jedis = connection.connect();
      Set<String> keys = jedis.keys(keyPattern);
      keysIterator = keys.iterator();
      return advance();
    }

    @Override
    public boolean advance() {
      if (keysIterator.hasNext()) {
        String key = keysIterator.next();
        String value = jedis.get(key);
        KV<String, String> kv = KV.of(key, value);
        current = kv;
        return true;
      }
      return false;
    }

    @Override
    public void close() {
      jedis.quit();
    }

    @Override
    public KV<String, String> getCurrent() throws NoSuchElementException {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current;
    }

  }

  @Override
  public Reader createReader(String keyPattern) throws IOException {
    return new RedisReaderImpl(keyPattern);
  }

  /**
   * The estimate size bytes is based on sampling, computing average size of 10 random
   * key/value pairs. This sampling average size is used with the Redis dbSize to get an
   * estimation of the actual database size.
   *
   * @return The estimated size of the Redis database in bytes.
   */
  @Override
  public long getEstimatedSizeBytes() {
    Jedis jedis = connection.connect();
    // estimate the size of a key/value pair using sampling
    long samplingSize = 0;
    for (int i = 0; i < 10; i++) {
      String key = jedis.randomKey();
      if (key != null) {
        samplingSize = samplingSize + key.getBytes().length;
        String value = jedis.get(key);
        if (value != null) {
          samplingSize = samplingSize + value.getBytes().length;
        }
      }
    }
    long samplingAverage = samplingSize / 10;
    // db size
    long dbSize = jedis.dbSize();
    jedis.quit();
    return dbSize * samplingAverage;
  }

}
