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
import java.io.StringReader;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.beam.sdk.transforms.display.DisplayData;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;

/**
 * {@code RedisConnection} describes and wraps a connection to Redis server or cluster.
 */
@AutoValue
public abstract class RedisConnection implements Serializable {

  @Nullable abstract List<String> nodes();
  @Nullable abstract String password();
  abstract int timeout();

  abstract Builder builder();

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setNodes(List<String> nodes);
    abstract Builder setPassword(String password);
    abstract Builder setTimeout(int timeout);
    abstract RedisConnection build();
  }

  public static RedisConnection create(List<String> nodes) {
    checkArgument(nodes != null, "RedisConnection.create(nodes) called with null nodes list");
    checkArgument(nodes.size() > 0, "RedisConnection.create(nodes) called with empty nodes list");
    for (String node : nodes) {
      checkArgument(node.contains(":"), "Invalid node syntax (not host:port): " + node);
    }
    return new AutoValue_RedisConnection.Builder().setNodes(nodes).setTimeout(3600).build();
  }

  public RedisConnection withPassword(String password) {
    checkArgument(password != null, "RedisConnection.create(nodes).withPassword(password) "
        + "called with null password");
    return builder().setPassword(password).build();
  }

  public RedisConnection withTimeout(int timeout) {
    return builder().setTimeout(timeout).build();
  }

  public Jedis connect() {
    String instance = nodes().get(0);
    String[] uri = instance.split(":");
    return new Jedis(uri[0], Integer.parseInt(uri[1]), timeout());
  }

  public JedisCommands connectCluster() throws Exception {
    Jedis jedis = connect();
    if (isClusterEnabled(jedis)) {
      Set<HostAndPort> jedisNodes = new HashSet<>();
      for (String clusterNode : nodes()) {
        String[] c = clusterNode.split(":");
        jedisNodes.add(new HostAndPort(c[0], Integer.parseInt(c[1])));
      }
      return new JedisCluster(jedisNodes, timeout());
    }
    return null;
  }

  protected Properties info(Jedis jedis) throws Exception {
    String info = jedis.info();
    jedis.info("cluster_enabled");
    Properties properties = new Properties();
    properties.load(new StringReader(info));
    return properties;
  }

  protected boolean isClusterEnabled(Jedis jedis) throws Exception {
    Properties properties = info(jedis);
    String value = properties.getProperty("cluster_enabled");
    if (value != null && !value.equals("0")) {
      return true;
    }
    return false;
  }

  public void populateDisplayData(DisplayData.Builder builder) {
    StringBuilder stringBuilder = new StringBuilder();
    for (String node : nodes()) {
      stringBuilder.append(node).append(" ");
    }
    builder.addIfNotNull(DisplayData.item("nodes", stringBuilder.toString()));
    builder.addIfNotNull(DisplayData.item("timeout", timeout()));
  }

}
