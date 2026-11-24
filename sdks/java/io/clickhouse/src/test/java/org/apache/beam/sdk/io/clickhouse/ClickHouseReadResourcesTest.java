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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests late-response ownership without timing-dependent network delays. */
@RunWith(JUnit4.class)
public class ClickHouseReadResourcesTest {
  @Test
  public void successfulAwaitTransfersOwnershipToCaller() throws Exception {
    TrackedResponse response = new TrackedResponse(false);
    assertSame(
        response, ClickHouseReadResources.await(CompletableFuture.completedFuture(response)));
    assertEquals(0, response.closes.get());
    response.close();
    assertEquals(1, response.closes.get());
  }

  @Test(timeout = 10000)
  public void interruptedAwaitClosesLateResponseAndPreservesInterruptFlag() throws Exception {
    verifyLateClose(false);
  }

  @Test(timeout = 10000)
  public void lateCloseFailureDoesNotEscapeOrRetainUnsafeClientCause() throws Exception {
    verifyLateClose(true);
  }

  private static void verifyLateClose(boolean closeFails) throws Exception {
    CompletableFuture<TrackedResponse> future = new CompletableFuture<>();
    CountDownLatch started = new CountDownLatch(1);
    AtomicBoolean interrupted = new AtomicBoolean();
    AtomicBoolean unexpected = new AtomicBoolean();
    Thread waiter =
        new Thread(
            () -> {
              started.countDown();
              try {
                ClickHouseReadResources.await(future);
                unexpected.set(true);
              } catch (InterruptedException expected) {
                interrupted.set(Thread.currentThread().isInterrupted());
              } catch (Exception e) {
                unexpected.set(true);
              }
            });
    waiter.start();
    assertTrue(started.await(5, TimeUnit.SECONDS));
    waiter.interrupt();
    waiter.join(5000);
    assertFalse(waiter.isAlive());
    assertTrue(interrupted.get());
    assertFalse(unexpected.get());
    assertFalse(future.isCancelled());
    TrackedResponse response = new TrackedResponse(closeFails);
    assertTrue(future.complete(response));
    assertEquals(1, response.closes.get());
  }

  private static final class TrackedResponse implements AutoCloseable {
    private final AtomicInteger closes = new AtomicInteger();
    private final boolean fail;

    private TrackedResponse(boolean fail) {
      this.fail = fail;
    }

    @Override
    public void close() throws IOException {
      closes.incrementAndGet();
      if (fail) {
        throw new IOException("private-client-error");
      }
    }
  }
}
