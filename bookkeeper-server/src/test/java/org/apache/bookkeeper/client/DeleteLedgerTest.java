package org.apache.bookkeeper.client;

import org.apache.bookkeeper.utils.ResultType;
import org.apache.bookkeeper.versioning.Version;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class DeleteLedgerTest extends BookKeeperTest {

    private long ledgerId;
    private boolean closedClient;

    public DeleteLedgerTest(boolean closedClient, long ledgerId, ResultType resultType) {
        configure(closedClient, ledgerId, resultType);
    }

    private void configure(boolean closedClient, long ledgerId, ResultType resultType) {
        this.closedClient = closedClient;
        this.ledgerId = ledgerId;
        this.ensSize = 1;
        this.writeQuorumSize = 1;
        this.ackQuorumSize = 1;
        this.digestType = BookKeeper.DigestType.DUMMY;
        this.passwd = "password".getBytes(StandardCharsets.UTF_8);

        configureResult(resultType);
    }

    @Override
    protected void configureResult(ResultType resultType) {
        super.configureResult(resultType);
        if (resultType == ResultType.BK_ERR && closedClient) {
            this.expectedError = new BKException.BKClientClosedException();
        }
    }

    @Before
    public void simulatePreviousCreation() {
        if (ledgerId == 4113L) {
            when(ledgerManager.removeLedgerMetadata(anyLong(), any(Version.class))).thenReturn(whenCompleteRemoveOk);
        } else {
            when(ledgerManager.removeLedgerMetadata(anyLong(), any(Version.class))).thenReturn(whenCompleteRemoveWrong);
        }
    }

    @Test
    public void deleteTest() throws BKException, InterruptedException {
        LedgerHandle handle = null;
        if (ledgerId == 4113L) {
            try {
                handle = bk.createLedger(ensSize, writeQuorumSize, ackQuorumSize, digestType, passwd);
                handle.close();
                assertTrue(handle.isClosed());
                assertEquals(ledgerId, handle.getId());
            } catch (InterruptedException | BKException e) {
                e.printStackTrace();
            }
        }

        if (closedClient) {
            /* 2nd Iteration: increment statement and condition coverage */
            bk.close();
        }

        if (handle != null) {
            // ledger exists
            Logger.getGlobal().log(Level.WARNING, "ledger exists");
            try {
                bk.deleteLedger(ledgerId);
            } catch (Exception e) {
                e.printStackTrace();
                assertEquals(expectedError.getClass(), e.getClass());
            }
        } else {
            // ledger not exists
            Logger.getGlobal().log(Level.WARNING, "ledger not exists");
            try {
                bk.deleteLedger(ledgerId);
            } catch (Exception e) {
                e.printStackTrace();
                assertEquals(expectedError.getClass(), e.getClass());
            }
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                // {clientClosed, ledgerId, resultType}
                /* 1st Iteration: category partition */
                {false, 4113L, ResultType.OK},
                {false, 0, ResultType.BK_ERR},
                {false, -1, ResultType.BK_ERR},

                /* 2nd Iteration: increment statement and condition coverage */
                {true, 4113L, ResultType.BK_ERR}
        });
    }
}
