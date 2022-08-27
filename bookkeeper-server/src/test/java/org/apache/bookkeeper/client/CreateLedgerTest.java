package org.apache.bookkeeper.client;

import org.apache.bookkeeper.utils.ParamType;
import org.apache.bookkeeper.utils.ResultType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.MockedStatic;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mockStatic;

@RunWith(Parameterized.class)
public class CreateLedgerTest extends BookKeeperTest {

    private LedgerHandle ledger;
    private boolean clientClosed;
    private boolean creationTimeout;

    public CreateLedgerTest(boolean clientClosed, boolean creationTimeout,
                            int ensSize, int writeQuorumSize, int ackQuorumSize,
                            BookKeeper.DigestType digestType, ParamType passwdType, ResultType expectedType) {
        configure(clientClosed, creationTimeout, ensSize, writeQuorumSize, ackQuorumSize, digestType, passwdType, expectedType);

    }

    private void configure(boolean clientClosed, boolean creationTimeout,
                           int ensSize, int writeQuorumSize, int ackQuorumSize,
                           BookKeeper.DigestType digestType, ParamType passwdType, ResultType expectedType) {
        this.clientClosed = clientClosed;
        this.creationTimeout = creationTimeout;
        if (creationTimeout) {
            syncCallbackUtils = mockStatic(SyncCallbackUtils.class);
        }
        configureQuorum(ensSize, writeQuorumSize, ackQuorumSize);
        configureDigestType(digestType);
        configureCreationPasswd(passwdType);
        configureResult(expectedType);
    }

    @Override
    protected void configureResult(ResultType resultType) {
        super.configureResult(resultType);
        if (resultType == ResultType.BK_ERR) {
            this.expectedError = new BKException.BKClientClosedException();
        } else if (resultType == ResultType.UNEXPECTED_ERR) {
            this.expectedError = new BKException.BKUnexpectedConditionException();
        }
    }

    protected void configureCreationPasswd(ParamType passwdType) {
        String pass;
        switch (passwdType) {
            case EMPTY:
                pass = "";
                this.passwd = pass.getBytes(StandardCharsets.UTF_8);
                break;
            case VALID:
                pass = "password";
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
                // {clientClosed, creationTimeout, ensSize, Qw, Qa, DigestType, Passwd, ExpectedResult}
                /* First Iteration: category partition */
                // fixme: ensSize = 0 is not handled by the method!
//                {false, false, 0, 0, 0, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
//                {false, false, 0, 0, 1, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
//                {false, false, 0, 0, -1, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
//                {false, false, 0, 1, 1, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
//                {false, false, 0, 1, 2, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
//                {false, false, 0, 1, 0, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
//                {false, false, 0, -1, -1, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
//                {false, false, 0, -1, 0, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
//                {false, false, 0, -1, -2, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {false, false, 1, 1, 1, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.OK},
                {false, false, 1, 1, 2, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {false, false, 1, 1, 0, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.OK},
                {false, false, 1, 2, 2, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {false, false, 1, 2, 3, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {false, false, 1, 2, 1, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {false, false, 1, 0, 0, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.OK},
                {false, false, 1, 0, 1, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                // fixme: negative values for Qa non handled by the method!
//                {false, false, 1, 0, -1, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.OK},
                {false, false, 4, 4, 4, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.OK},
                {false, false, 4, 4, 5, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {false, false, 4, 4, 3, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.OK},
                {false, false, 4, 5, 5, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {false, false, 4, 5, 6, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {false, false, 4, 5, 4, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {false, false, 4, 3, 3, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.OK},
                {false, false, 4, 3, 4, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {false, false, 4, 3, 2, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.OK},
                {false, false, 4, 3, 2, BookKeeper.DigestType.MAC, ParamType.VALID, ResultType.OK},
                {false, false, 4, 3, 2, BookKeeper.DigestType.CRC32, ParamType.VALID, ResultType.OK},
                {false, false, 4, 3, 2, BookKeeper.DigestType.CRC32C, ParamType.VALID, ResultType.OK},
                {false, false, 4, 3, 2, BookKeeper.DigestType.DUMMY, ParamType.EMPTY, ResultType.OK},
                // fixme: null password not handled by the method!
//                {false, false, 4, 3, 2, BookKeeper.DigestType.DUMMY, ParamType.NULL, ResultType.ILLEGAL_ARG_ERR},

                /* Second iteration: increment statement coverage */
                {true, false, 4, 3, 2, BookKeeper.DigestType.MAC, ParamType.VALID, ResultType.BK_ERR},

                /* Third iteration: data flow coverage (+2 def-use covered)*/
                {false, true, 4, 3, 2, BookKeeper.DigestType.MAC, ParamType.VALID, ResultType.UNEXPECTED_ERR}
        });
    }

    @Before
    public void mockBehaviour() {
        /* 2nd iteration: trigger timeout with lh == null */
        if (creationTimeout) {
            try {
                syncCallbackUtils.when(SyncCallbackUtils.waitForResult(any())).thenReturn(null);
            } catch (InterruptedException | BKException e) {
                e.printStackTrace();
            }
        }
    }

    @After
    public void cleanMockStatics() {
        if (creationTimeout) syncCallbackUtils.close();
    }

    @Test
    public void createTest() throws BKException, InterruptedException {
        if (clientClosed) {
            // close the client
            bk.close();
        }
        try {
            ledger = bk.createLedger(ensSize, writeQuorumSize, ackQuorumSize, digestType, passwd);
            assertNotNull(ledger);
            assertEquals(expected.getLedgerId(), ledger.ledgerId);
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals(expectedError.getClass(), e.getClass());
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
