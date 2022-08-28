package org.apache.bookkeeper.client;

import org.apache.bookkeeper.utils.ParamType;
import org.apache.bookkeeper.utils.ResultType;
import org.junit.After;
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
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class OpenLedgerTest extends BookKeeperTest {

    private long ledgerId;
    private ParamType passType;
    private LedgerHandle ledger;
    private boolean clientClosed;

    public OpenLedgerTest(boolean clientClosed, long ledgerId, BookKeeper.DigestType digestType, ParamType passwdType, ResultType resultType) {
        configure(clientClosed, ledgerId, digestType, passwdType, resultType);
    }

    private void configure(boolean clientClosed, long ledgerId, BookKeeper.DigestType digestType, ParamType passwdType, ResultType resultType) {
        this.clientClosed = clientClosed;
        this.ledgerId = ledgerId;
        this.ensSize = 1;
        this.writeQuorumSize = 1;
        this.ackQuorumSize = 1;
        configureDigestType(digestType);
        configureOpeningPasswd(passwdType);
        configureResult(resultType);
    }

    @Override
    protected void configureResult(ResultType resultType) {
        super.configureResult(resultType);
        if (resultType == ResultType.BK_ERR && clientClosed) {
            this.expectedError = new BKException.BKClientClosedException();
        }
    }

    private void configureOpeningPasswd(ParamType passwdType) {
        String pass;
        passType = passwdType;
        switch (passwdType) {
            case EMPTY:
                pass = "";
                this.passwd = pass.getBytes(StandardCharsets.UTF_8);
                break;
            case INCORRECT:
                pass = "NOTPASSWORD";
                this.passwd = pass.getBytes(StandardCharsets.UTF_8);
                break;
            case CORRECT:
                pass = "PASSWORD";
                this.passwd = pass.getBytes(StandardCharsets.UTF_8);
                break;
            case NULL:
                this.passwd = null;
                break;
            default:
                break;
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                // {clientClosed, lId, DigestType, passwd, result}
                /* 1st Iteration: category partition */
                {false, 4113L, BookKeeper.DigestType.DUMMY, ParamType.CORRECT, ResultType.OK},
                {false, 2001L, BookKeeper.DigestType.DUMMY, ParamType.CORRECT, ResultType.BK_ERR},
                {false, 4113L, BookKeeper.DigestType.CRC32, ParamType.CORRECT, ResultType.OK},
                {false, 4113L, BookKeeper.DigestType.MAC, ParamType.CORRECT, ResultType.OK},
                {false, 4113L, BookKeeper.DigestType.CRC32C, ParamType.CORRECT, ResultType.OK},
                {false, 4113L, BookKeeper.DigestType.DUMMY, ParamType.EMPTY, ResultType.OK},
                {false, 4113L, BookKeeper.DigestType.DUMMY, ParamType.INCORRECT, ResultType.ILLEGAL_ARG_ERR},
//                {-1, BookKeeper.DigestType.DUMMY, ParamType.CORRECT, ResultType.BK_ERR}

                /* 2nd Iteration: increment statement coverage */
                {true, 4113L, BookKeeper.DigestType.MAC, ParamType.CORRECT, ResultType.BK_ERR}
        });
    }

    @Before
    public void simulatePreviousCreation() {
        // to open ledger, note that readLedgerMetadata must return a Future which, when completed,
        // contains the requested versioned metadata completed with an exception::
        //     * BKException.BKNoSuchLedgerExistsOnMetadataServerException} if ledger not exist
        if (ledgerId == 4113L) {
            when(ledgerManager.readLedgerMetadata(ledgerId)).thenReturn(whenCompleteCloseOk); // ledger exists
        } else {
            when(ledgerManager.readLedgerMetadata(ledgerId))
                    .thenReturn(whenCompleteCloseWrong); //ledger not exists
        }
    }

    @Test
    public void openTest() throws BKException, InterruptedException {
        if (ledgerId == 4113L && passType == ParamType.CORRECT) {
            try {
                ledger = bk.createLedger(ensSize, writeQuorumSize, ackQuorumSize, digestType, passwd);
                ledger.close();
                assertTrue(ledger.isClosed());
                if (ledger.isClosed()) Logger.getGlobal().log(Level.WARNING, "ledger closed.");
            } catch (InterruptedException | BKException e) {
                e.printStackTrace();
            }
        }

        if (clientClosed) {
            /* 2nd Iteration: increment statement and condition coverage */
            bk.close();
        }

        if (ledger == null){
            // ledger does not exists
            try {
                ledger = bk.openLedger(ledgerId, digestType, passwd);
                assertNotNull(ledger);
            }catch (BKException | InterruptedException e){
                assertEquals(expectedError.getClass(), e.getClass());
            }
        } else {
            // ledger exists and was previously closed
            try {
                ledger = bk.openLedger(ledgerId, digestType, passwd);
                assertNotNull(ledger);
                assertEquals(expected.getLedgerId(), ledger.ledgerId);
            } catch (BKException | InterruptedException e) {
                e.printStackTrace();
                assertEquals(expectedError.getClass(), e.getClass());
            }
        }
    }

    @After
    public void closeLedger() {
        if (!clientClosed) {
            try {
                if (ledger != null) ledger.close(); //close properly
            } catch (Exception e) {
                e.printStackTrace();
                Logger.getGlobal().log(Level.WARNING, "ledger not properly closed");
            }
        }
    }
}
