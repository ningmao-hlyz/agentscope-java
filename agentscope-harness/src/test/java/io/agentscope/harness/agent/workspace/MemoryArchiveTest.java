/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.agentscope.harness.agent.workspace;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.harness.agent.filesystem.AbstractFilesystem;
import io.agentscope.harness.agent.filesystem.AtomicTextFile;
import io.agentscope.harness.agent.filesystem.OverlayFilesystem;
import io.agentscope.harness.agent.filesystem.local.LocalFilesystem;
import io.agentscope.harness.agent.filesystem.remote.RemoteFilesystem;
import io.agentscope.harness.agent.filesystem.remote.store.InMemoryStore;
import io.agentscope.harness.agent.filesystem.remote.store.StoreItem;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class MemoryArchiveTest {
    private static final String SOURCE = "memory/2000-01-01.md";
    private static final String ARCHIVE = "memory/archive/2000-01-01.md";
    private static final String CONTENT = "- original fact";
    private final RuntimeContext rc = RuntimeContext.empty();
    @TempDir Path workspace;

    private AbstractFilesystem filesystem(boolean remote) {
        return remote
                ? new RemoteFilesystem(new InMemoryStore(), List.of("archive"))
                : new LocalFilesystem(workspace);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void unchangedSnapshotArchivesAndSurvivesRecreation(boolean remote) {
        AbstractFilesystem fs = filesystem(remote);
        try (WorkspaceManager manager = new WorkspaceManager(workspace, fs)) {
            manager.writeMemoryFileUtf8(rc, SOURCE, CONTENT);
            assertTrue(manager.archiveMemoryFile(rc, SOURCE, CONTENT));
        }
        try (WorkspaceManager rebuilt = new WorkspaceManager(workspace, fs)) {
            assertNull(rebuilt.atomicMemoryFile(rc, SOURCE).read());
            assertEquals(CONTENT, rebuilt.atomicMemoryFile(rc, ARCHIVE).read());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void appendAfterArchiveCopyKeepsTheNewFactActive(boolean remote) {
        AbstractFilesystem fs = filesystem(remote);
        try (WorkspaceManager peer = new WorkspaceManager(workspace, fs);
                WorkspaceManager manager =
                        new WorkspaceManager(workspace, fs) {
                            @Override
                            public AtomicTextFile atomicMemoryFile(
                                    RuntimeContext context, String path) {
                                AtomicTextFile delegate = super.atomicMemoryFile(context, path);
                                if (!ARCHIVE.equals(path)) return delegate;
                                return new AtomicTextFile() {
                                    public String read() {
                                        return delegate.read();
                                    }

                                    public boolean compareAndSet(String expected, String updated) {
                                        boolean copied = delegate.compareAndSet(expected, updated);
                                        if (copied)
                                            peer.appendMemoryFileUtf8(
                                                    context, SOURCE, "\n- late fact");
                                        return copied;
                                    }
                                };
                            }
                        }) {
            manager.writeMemoryFileUtf8(rc, SOURCE, CONTENT);
            assertFalse(manager.archiveMemoryFile(rc, SOURCE, CONTENT));
            assertEquals(CONTENT + "\n- late fact", manager.atomicMemoryFile(rc, SOURCE).read());
            assertEquals(CONTENT, manager.atomicMemoryFile(rc, ARCHIVE).read());
            String complete = CONTENT + "\n- late fact";
            assertTrue(peer.archiveMemoryFile(rc, SOURCE, complete));
            assertNull(peer.atomicMemoryFile(rc, SOURCE).read());
            assertEquals(complete, peer.atomicMemoryFile(rc, ARCHIVE).read());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void archiveWriteFailureKeepsSource(boolean remote) {
        AbstractFilesystem fs = filesystem(remote);
        try (WorkspaceManager manager =
                new WorkspaceManager(workspace, fs) {
                    @Override
                    public AtomicTextFile atomicMemoryFile(RuntimeContext context, String path) {
                        if (!ARCHIVE.equals(path)) return super.atomicMemoryFile(context, path);
                        return new AtomicTextFile() {
                            public String read() {
                                return null;
                            }

                            public boolean compareAndSet(String expected, String updated) {
                                throw new IllegalStateException("archive unavailable");
                            }
                        };
                    }
                }) {
            manager.writeMemoryFileUtf8(rc, SOURCE, CONTENT);
            assertFalse(manager.archiveMemoryFile(rc, SOURCE, CONTENT));
            assertEquals(CONTENT, manager.atomicMemoryFile(rc, SOURCE).read());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void failedDeleteResumesFromIdenticalCopyAfterRestart(boolean remote) {
        AbstractFilesystem fs = filesystem(remote);
        try (WorkspaceManager manager =
                new WorkspaceManager(workspace, fs) {
                    @Override
                    public AtomicTextFile atomicMemoryFile(RuntimeContext context, String path) {
                        AtomicTextFile delegate = super.atomicMemoryFile(context, path);
                        if (!SOURCE.equals(path)) return delegate;
                        return new AtomicTextFile() {
                            public String read() {
                                return delegate.read();
                            }

                            public boolean compareAndSet(String expected, String updated) {
                                return delegate.compareAndSet(expected, updated);
                            }

                            public boolean compareAndDelete(String expected) {
                                throw new IllegalStateException("delete unavailable");
                            }
                        };
                    }
                }) {
            manager.writeMemoryFileUtf8(rc, SOURCE, CONTENT);
            assertFalse(manager.archiveMemoryFile(rc, SOURCE, CONTENT));
            assertEquals(CONTENT, manager.atomicMemoryFile(rc, SOURCE).read());
            assertEquals(CONTENT, manager.atomicMemoryFile(rc, ARCHIVE).read());
        }
        try (WorkspaceManager rebuilt = new WorkspaceManager(workspace, fs)) {
            assertTrue(rebuilt.archiveMemoryFile(rc, SOURCE, CONTENT));
            assertNull(rebuilt.atomicMemoryFile(rc, SOURCE).read());
            assertEquals(CONTENT, rebuilt.atomicMemoryFile(rc, ARCHIVE).read());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void differentExistingArchiveKeepsBothFiles(boolean remote) {
        try (WorkspaceManager manager = new WorkspaceManager(workspace, filesystem(remote))) {
            manager.writeMemoryFileUtf8(rc, SOURCE, CONTENT);
            manager.writeMemoryFileUtf8(rc, ARCHIVE, "- older archive");
            assertFalse(manager.archiveMemoryFile(rc, SOURCE, CONTENT));
            assertEquals(CONTENT, manager.atomicMemoryFile(rc, SOURCE).read());
            assertEquals("- older archive", manager.atomicMemoryFile(rc, ARCHIVE).read());
        }
    }

    @Test
    void remoteDeleteCannotRemoveRecreatedContentWithReusedVersion() {
        List<String> ns = List.of("archive");
        InMemoryStore store =
                new InMemoryStore() {
                    @Override
                    public boolean deleteIfUnchanged(
                            List<String> namespace, String key, StoreItem expected) {
                        super.delete(namespace, key);
                        super.put(
                                namespace,
                                key,
                                Map.of("content", "- recreated", "encoding", "utf-8"));
                        assertEquals(expected.version(), get(namespace, key).version());
                        return super.deleteIfUnchanged(namespace, key, expected);
                    }
                };
        try (WorkspaceManager manager =
                new WorkspaceManager(workspace, new RemoteFilesystem(store, ns))) {
            manager.writeMemoryFileUtf8(rc, SOURCE, CONTENT);
            assertFalse(manager.archiveMemoryFile(rc, SOURCE, CONTENT));
            assertEquals("- recreated", manager.atomicMemoryFile(rc, SOURCE).read());
            assertEquals(CONTENT, manager.atomicMemoryFile(rc, ARCHIVE).read());
        }
    }

    @Test
    void unavailableConditionalDeleteNeverFallsBackToPlainDelete() {
        InMemoryStore store =
                new InMemoryStore() {
                    @Override
                    public boolean deleteIfUnchanged(
                            List<String> ns, String key, StoreItem expected) {
                        return false;
                    }

                    @Override
                    public void delete(List<String> ns, String key) {
                        throw new AssertionError("unconditional delete");
                    }
                };
        try (WorkspaceManager manager =
                new WorkspaceManager(workspace, new RemoteFilesystem(store, List.of("archive")))) {
            manager.writeMemoryFileUtf8(rc, SOURCE, CONTENT);
            assertFalse(manager.archiveMemoryFile(rc, SOURCE, CONTENT));
            assertEquals(CONTENT, manager.atomicMemoryFile(rc, SOURCE).read());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void overlayNeverRevealsASharedLedgerByDeletingThePrivateCopy(boolean sharedSource) {
        InMemoryStore store = new InMemoryStore();
        var lower = new RemoteFilesystem(store, List.of("shared"));
        if (sharedSource) lower.write(rc, SOURCE, "- shared source");
        var fs = new OverlayFilesystem(new RemoteFilesystem(store, List.of("private")), lower);
        try (WorkspaceManager manager = new WorkspaceManager(workspace, fs)) {
            manager.writeMemoryFileUtf8(rc, SOURCE, CONTENT);
            assertEquals(!sharedSource, manager.archiveMemoryFile(rc, SOURCE, CONTENT));
            assertEquals(
                    sharedSource ? CONTENT : null, manager.atomicMemoryFile(rc, SOURCE).read());
            assertEquals(CONTENT, manager.atomicMemoryFile(rc, ARCHIVE).read());
            if (sharedSource)
                assertEquals("- shared source", lower.atomicTextFile(rc, SOURCE, true).read());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void concurrentArchiveAndAppendNeverLoseTheAppendedFact(boolean remote) throws Exception {
        AbstractFilesystem fs = filesystem(remote);
        try (WorkspaceManager first = new WorkspaceManager(workspace, fs);
                WorkspaceManager second = new WorkspaceManager(workspace, fs)) {
            var executor = Executors.newFixedThreadPool(2);
            try {
                for (int i = 0; i < 10; i++) {
                    String path = "memory/race-" + i + ".md";
                    first.writeMemoryFileUtf8(rc, path, CONTENT);
                    CountDownLatch start = new CountDownLatch(1);
                    var append =
                            executor.submit(
                                    () -> {
                                        assertTrue(start.await(5, TimeUnit.SECONDS));
                                        second.appendMemoryFileUtf8(
                                                rc, path, "\n- concurrent fact");
                                        return true;
                                    });
                    var archive =
                            executor.submit(
                                    () -> {
                                        assertTrue(start.await(5, TimeUnit.SECONDS));
                                        return first.archiveMemoryFile(rc, path, CONTENT);
                                    });
                    start.countDown();
                    assertTrue(append.get(10, TimeUnit.SECONDS));
                    archive.get(10, TimeUnit.SECONDS);
                    String active = first.atomicMemoryFile(rc, path).read();
                    assertTrue(active.contains("- concurrent fact"));
                    String saved =
                            first.atomicMemoryFile(rc, path.replace("memory/", "memory/archive/"))
                                    .read();
                    assertTrue(active.contains(CONTENT) || CONTENT.equals(saved));
                }
            } finally {
                executor.shutdownNow();
            }
        }
    }

    @Test
    void recreationDuringIndexRemovalKeepsTheNewFileDiscoverable() {
        var fs = filesystem(false);
        try (WorkspaceIndex index = spy(WorkspaceIndex.open(workspace));
                WorkspaceManager manager = new WorkspaceManager(workspace, fs, index);
                WorkspaceManager peer = new WorkspaceManager(workspace, fs, index)) {
            manager.writeMemoryFileUtf8(rc, SOURCE, CONTENT);
            doAnswer(
                            invocation -> {
                                peer.appendMemoryFileUtf8(rc, SOURCE, "- recreated fact");
                                return invocation.callRealMethod();
                            })
                    .when(index)
                    .remove(SOURCE);
            assertTrue(manager.archiveMemoryFile(rc, SOURCE, CONTENT));
            assertEquals("- recreated fact", manager.atomicMemoryFile(rc, SOURCE).read());
            assertTrue(index.exists(SOURCE));
            assertEquals(CONTENT, manager.atomicMemoryFile(rc, ARCHIVE).read());
        }
    }
}
