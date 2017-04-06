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
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An IO to manipulate Redis key/value database.
 *
 * <h3>Reading Redis key/value pairs</h3>
 *
 * <p>RedisIO.Read provides a source which returns a bounded {@link PCollection} containing
 * key/value pairs as {@code KV<String, String>}.
 *
 * <p>To configure a Redis source, you have to provide Redis server hostname and port number.
 * Optionally, you can provide a key pattern (to filter the keys). The following example
 * illustrates how to configure a source:
 *
 * <pre>{@code
 *
 *  pipeline.apply(RedisIO.read()
 *    .withConnectionConfiguration(
 *      RedisConnectionConfiguration.create().withHost("localhost").withPort(6379))
 *    .withKeyPattern("foo*")
 *
 * }</pre>
 */
public class RedisIO {

  private static final Logger LOG = LoggerFactory.getLogger(RedisIO.class);

  public static Read read() {
    return new  AutoValue_RedisIO_Read.Builder().build();
  }

  private RedisIO() {
  }

  /**
   * A {@link PTransform} reading key/value pairs from a Redis database.
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<KV<String, String>>> {

    @Nullable abstract RedisConnectionConfiguration connectionConfiguration();
    @Nullable abstract String keyPattern();
    @Nullable abstract RedisService redisService();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      @Nullable abstract Builder setConnectionConfiguration(
          RedisConnectionConfiguration connection);
      @Nullable abstract Builder setKeyPattern(String keyPattern);
      @Nullable abstract Builder setRedisService(RedisService redisService);
      abstract Read build();
    }

    /**
     * Define the connectionConfiguration to the Redis server.
     *
     * @param connection The {@link RedisConnectionConfiguration}.
     * @return The corresponding {@link Read} {@link PTransform}.
     */
    public Read withConnectionConfiguration(RedisConnectionConfiguration connection) {
      checkArgument(connection != null, "RedisIO.read().withConnectionConfiguration"
          + "(connectionConfiguration) called with null connectionConfiguration");
      return builder().setConnectionConfiguration(connection).build();
    }

    public Read withKeyPattern(String keyPattern) {
      checkArgument(keyPattern != null, "RedisIO.read().withKeyPattern(keyPattern) called with "
          + "null keyPattern");
      return builder().setKeyPattern(keyPattern).build();
    }

    /**
     * Define the Redis service to use.
     *
     * @param redisService The {@link RedisService} to use.
     * @return The corresponding {@link Read} {@link PTransform}.
     */
    public Read withRedisService(RedisService redisService) {
      checkArgument(redisService != null, "RedisIO.read().withRedisService(service) called with"
          + " null service");
      return builder().setRedisService(redisService).build();
    }

    @Override
    public void validate(PBegin input) {
      checkState(connectionConfiguration() != null || redisService() != null,
          "RedisIO.read() requires a connectionConfiguration to be set "
              + "withConnection(connectionConfiguration) or a service to be set withRedisService"
              + "(service)");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      if (connectionConfiguration() != null) {
        connectionConfiguration().populateDisplayData(builder);
      }
    }

    @Override
    public PCollection<KV<String, String>> expand(PBegin input) {
      return input.getPipeline()
          .apply(org.apache.beam.sdk.io.Read.from(
              new RedisSource(
                  keyPattern(),
                  new SerializableFunction<PipelineOptions, RedisService>() {
                    @Override
                    public RedisService apply(PipelineOptions pipelineOptions) {
                      return getRedisService(pipelineOptions);
                    }
                  }, null)));
    }

    /**
     * Helper function to either get a fake/mock Redis service provided by
     * {@link #withRedisService(RedisService)} or creates and returns an implementation of a
     * concrete Redis service dealing with an actual Redis server.
     */
    @VisibleForTesting
    RedisService getRedisService(PipelineOptions pipelineOptions) {
      if (redisService() != null) {
        return redisService();
      }
      return new RedisServiceImpl(connectionConfiguration());
    }

  }

  /**
   * A bounded source reading key-value pairs from a Redis server.
   */
  @VisibleForTesting
  protected static class RedisSource extends BoundedSource<KV<String, String>> {

    private final String keyPattern;
    private final SerializableFunction<PipelineOptions, RedisService> serviceFactory;
    private final RedisService.RedisNode node;

    protected RedisSource(String keyPattern,
                          SerializableFunction<PipelineOptions, RedisService> serviceFactory,
                          RedisService.RedisNode node) {
      this.keyPattern = keyPattern;
      this.serviceFactory = serviceFactory;
      this.node = node;
    }

    @Override
    public List<RedisSource> splitIntoBundles(long desiredBundleSizeBytes,
                                              PipelineOptions pipelineOptions) throws IOException {
      if (serviceFactory.apply(pipelineOptions).isClusterEnabled()) {
        LOG.info("Cluster detected");
        List<RedisSource> redisSources = new ArrayList<>();

        List<RedisService.RedisNode> nodes = serviceFactory.apply(pipelineOptions)
            .getClusterNodes();

        int slot = serviceFactory.apply(pipelineOptions).getKeySlot(keyPattern);

        for (RedisService.RedisNode node : nodes) {
          if (node.startSlot < slot && node.endSlot > slot) {
            redisSources.add(new RedisSource(keyPattern, serviceFactory, node));
          }
        }

        return redisSources;
      }
      LOG.info("Standalone instance detected, use a single source");
      // we don't have Redis cluster, so, we use an unique source
      return Collections.singletonList(this);
    }

    /**
     * The estimate size bytes is based on sampling, computing average size of 10 random
     * key/value pairs. This sampling average size is used with the Redis dbSize to get an
     * estimation of the actual database size.
     *
     * @param pipelineOptions The pipeline options.
     * @return The estimated size of the Redis database in bytes.
     */
    @Override
    public long getEstimatedSizeBytes(PipelineOptions pipelineOptions) throws IOException {
      return serviceFactory.apply(pipelineOptions).getEstimatedSizeBytes();
    }

    @Override
    public BoundedReader<KV<String, String>> createReader(PipelineOptions pipelineOptions) {
      return new RedisReader(this, serviceFactory.apply(pipelineOptions));
    }

    @Override
    public void validate() {
      // done in the Read
    }

    @Override
    public Coder<KV<String, String>> getDefaultOutputCoder() {
      return KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());
    }

  }

  private static class RedisReader extends BoundedSource.BoundedReader<KV<String, String>> {

    private final RedisSource source;
    private final RedisService service;
    private RedisService.Reader reader;

    public RedisReader(RedisSource source, RedisService service) {
      this.source = source;
      this.service = service;
    }

    @Override
    public boolean start() throws IOException {
      reader = service.createReader(source.keyPattern, source.node);
      return reader.start();
    }

    @Override
    public boolean advance() throws IOException {
      return reader.advance();
    }

    @Override
    public void close() throws IOException {
      reader.close();
      reader = null;
    }

    @Override
    public KV<String, String> getCurrent() {
      return reader.getCurrent();
    }

    @Override
    public RedisSource getCurrentSource() {
      return source;
    }

  }

}
