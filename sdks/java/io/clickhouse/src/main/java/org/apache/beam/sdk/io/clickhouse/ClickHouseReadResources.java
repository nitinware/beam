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
package org.apache.beam.sdk.io.clickhouse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/** Interruption-safe ownership of client responses returned asynchronously. */
final class ClickHouseReadResources {
  private ClickHouseReadResources() {}

  static void checkInterrupted() throws InterruptedException {
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedException("ClickHouse read interrupted");
    }
  }

  static <T extends AutoCloseable> T await(CompletableFuture<T> future)
      throws InterruptedException, ExecutionException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      // Cancelling a supplyAsync future does not stop its HTTP request, and can discard the
      // eventual
      // response. Keep the completion callback so a response arriving after interruption is closed.
      CompletableFuture<T> unusedCleanup =
          future.whenComplete(
              (response, failure) -> {
                if (response != null) {
                  try {
                    response.close();
                  } catch (Exception ignored) {
                    // Never log/attach late client errors, which may include SQL or credentials.
                  }
                }
              });
      Thread.currentThread().interrupt();
      throw e;
    }
  }
}
