package org.apache.bookkeeper.client;

import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.utils.ResultType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.*;
import org.mockito.stubbing.OngoingStubbing;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class ReadEntriesTest extends BookKeeperAdminTest {

    private boolean entriesOk;

    private long ledgerId;
    private long firstEntry;
    private long lastEntry;

    private long expectedEntries;
    private Exception expectedError;
    private Iterable<LedgerEntry> expected;

    public ReadEntriesTest(long ledgerId, long firstEntry, long lastEntry, ResultType resultType, long numEntries) {
        MockitoAnnotations.initMocks(this);
        syncCallbackUtils = Mockito.mockStatic(SyncCallbackUtils.class);
        configure(ledgerId, firstEntry, lastEntry, resultType, numEntries);
    }

    private void configure(long ledgerId, long firstEntry, long lastEntry, ResultType resultType, long numEntries) {
        this.ledgerId = ledgerId;
        this.firstEntry = firstEntry;
        this.lastEntry = lastEntry;
        this.expectedEntries = numEntries;
        configureResult(resultType);
    }

    private void configureResult(ResultType resultType) {
        switch (resultType) {
            case OK:
                LedgerEntry le = new LedgerEntry(LedgerEntryImpl.create(
                        4113L,
                        22L,
                        45L,
                        Unpooled.wrappedBuffer("questo testo è un test".getBytes(StandardCharsets.UTF_8))
                ));
                this.entriesOk = true;
                this.expected = new ArrayList<>(Collections.singleton(le));
                break;
            case ILLEGAL_ARG_ERR:
                this.expectedError = new IllegalArgumentException();
                break;
            case BK_ERR:
                this.expectedError = new BKException.BKNoSuchLedgerExistsException();
            default:
                break;
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {4113L, 0, 0, ResultType.OK, 1},
                {4113L, 0, -1, ResultType.OK, 3},
                {4113L, 0, 1, ResultType.OK, 2},
                {4113L, 1, 1, ResultType.OK, 1},
                {4113L, 1, 0, ResultType.ILLEGAL_ARG_ERR, 0},
                {4113L, 1, 2, ResultType.OK, 2},
                {4113L, -1, -1, ResultType.ILLEGAL_ARG_ERR, 0},
                {4113L, -1, -2, ResultType.ILLEGAL_ARG_ERR, 0},
                {4113L, -1, 0, ResultType.ILLEGAL_ARG_ERR, 0},
                {-1, 0, 0, ResultType.ILLEGAL_ARG_ERR, 0},
                {4111L, 0, 0, ResultType.BK_ERR, 0},
        });
    }

    @Before
    public void setUp() throws BKException, InterruptedException {
        if (ledgerId == 4113L) {
            /* valid ledger */
            doReturn(ledgerHandle).when(bookKeeperAdmin).openLedgerNoRecovery(ledgerId);
            doAnswer(invocationOnMock -> null)
                    .when(ledgerHandle)
                    .asyncReadEntriesInternal(anyLong(), anyLong(), any(), any(), anyBoolean());
            OngoingStubbing<Object> whenWaitForResult = syncCallbackUtils.when(
                    () -> SyncCallbackUtils.waitForResult(any())
            );

            if (entriesOk) {
                entries = new Enumeration<LedgerEntry>() {
                    long count = 0;

                    @Override
                    public boolean hasMoreElements() {
                        System.out.println(count);
                        return count < expectedEntries;
                    }

                    @Override
                    public LedgerEntry nextElement() {
                        if (count >= expectedEntries -1) {
                            syncCallbackUtils.when(() -> SyncCallbackUtils.waitForResult(any()))
                                    .thenThrow(new BKException.BKNoSuchEntryException());
                        }
                        count++;
                        return Mockito.mock(LedgerEntry.class);
                    }
                };
                whenWaitForResult.thenReturn(entries);
            } else {
                whenWaitForResult.thenThrow(new BKException.BKNoSuchEntryException());
            }
        } else {
            /* ledger not valid */
            doThrow(new BKException.BKNoSuchLedgerExistsException())
                    .when(bookKeeperAdmin)
                    .openLedgerNoRecovery(ledgerId);
        }
    }

    @Test
    public void testReadEntries() {
        try {
            int countEntries = 0;
            Iterable<LedgerEntry> ledgerEntries = bookKeeperAdmin.readEntries(ledgerId, firstEntry, lastEntry);
            for (LedgerEntry ledgerEntry : ledgerEntries) {
                countEntries++;
                System.out.println(ledgerEntry);
            }
            assertEquals(expectedEntries, countEntries);
        } catch (RuntimeException | InterruptedException | BKException e) {
            if (e.getCause() != null) {
                assertEquals(expectedError.getClass(), e.getCause().getClass());
            } else {
                assertEquals(expectedError.getClass(), e.getClass());
            }
        }
    }

    @After
    public void clean() {
        if (syncCallbackUtils != null) syncCallbackUtils.close();
    }
}
