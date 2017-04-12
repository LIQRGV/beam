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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.beam.sdk.transforms.display.DisplayData;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.util.SafeEncoder;

/**
 * {@code RedisConnectionConfiguration} describes and wraps a connectionConfiguration to Redis
 * server or cluster.
 */
@AutoValue
public abstract class RedisConnectionConfiguration implements Serializable {

  private static final int MASTER_NODE_INDEX = 2;

  @Nullable abstract String host();
  abstract int port();
  @Nullable abstract String auth();
  abstract int timeout();

  abstract Builder builder();

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setHost(String host);
    abstract Builder setPort(int port);
    abstract Builder setAuth(String auth);
    abstract Builder setTimeout(int timeout);
    abstract RedisConnectionConfiguration build();
  }

  public static RedisConnectionConfiguration create() {
    return new AutoValue_RedisConnectionConfiguration.Builder()
        .setHost(Protocol.DEFAULT_HOST)
        .setPort(Protocol.DEFAULT_PORT)
        .setTimeout(Protocol.DEFAULT_TIMEOUT).build();
  }

  public RedisConnectionConfiguration withHost(String host) {
    checkArgument(host != null, "RedisConnectionConfiguration.create().withHost(host) called "
        + "with empty host");
    return builder().setHost(host).build();
  }

  public RedisConnectionConfiguration withPort(int port) {
    checkArgument(port > 0, "RedisConnectionConfiguration.create().withPort(port) called with "
        + "invalid port number (" + port + ")");
    return builder().setPort(port).build();
  }

  public RedisConnectionConfiguration withAuth(String auth) {
    checkArgument(auth != null, "RedisConnectionConfiguration.create().withAuth(auth) called "
        + "with null auth");
    return builder().setAuth(auth).build();
  }

  public RedisConnectionConfiguration withTimeout(int timeout) {
    return builder().setTimeout(timeout).build();
  }

  /**
   * Connect to the Redis instance.
   */
  public Jedis connect() {
    return new Jedis(host(), port(), timeout());
  }

  protected boolean isClusterEnabled() {
    try (Jedis connection = connect()) {
      String[] infos = connection.info().split("\n");
      for (String info : infos) {
        if (info.startsWith("cluster_enabled:")) {
          if (info.substring("cluster_enabled:".length()).contains("1")) {
            return true;
          }
        }
      }
      return false;
    }
  }

  /**
   * Populate the display data with connectionConfiguration details.
   */
  public void populateDisplayData(DisplayData.Builder builder) {
    builder.addIfNotNull(DisplayData.item("host", host()));
    builder.addIfNotNull(DisplayData.item("port", port()));
    builder.addIfNotNull(DisplayData.item("timeout", timeout()));
  }

  public List<RedisService.RedisNode> getNodes() throws Exception {
    if (this.isClusterEnabled()) {
      return getClusterNodes();
    }
    return Collections.emptyList();
  }

  public List<RedisService.RedisNode> getClusterNodes() {
    try (Jedis jedis = connect()) {
      List<Object> slots = jedis.clusterSlots();

      for (Object slotInfoObj : slots) {
        List<Object> slotInfo = (List<Object>) slotInfoObj;

        int startSlot = Integer.parseInt(slotInfo.get(0).toString());
        int endSlot = Integer.parseInt(slotInfo.get(1).toString());

        if (slotInfo.size() <= MASTER_NODE_INDEX) {
          continue;
        }

        // host infos
        int size = slotInfo.size();
        for (int i = MASTER_NODE_INDEX; i < size; i++) {
          List<Object> hostInfos = (List<Object>) slotInfo.get(i);
          if (hostInfos.size() <= 0) {
            continue;
          }

          RedisService.RedisNode node = new RedisService.RedisNode();
          node.host = SafeEncoder.encode((byte[]) hostInfos.get(0));
          node.port = Integer.parseInt(hostInfos.get(1).toString());
          node.timeout = timeout();
          node.startSlot = startSlot;
          node.endSlot = endSlot;
          node.index = i;
          node.size = slotInfo.size() - 2;
        }
      }
    }
    return nodes;
  }

  private final List<RedisService.RedisNode> nodes = new ArrayList<>();


}
