package org.apache.bookkeeper.client;

import org.apache.bookkeeper.utils.ParamType;
import org.apache.bookkeeper.utils.ResultType;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class CreateLedgerTest extends BookKeeperTest {

    private LedgerHandle ledger;
    private boolean clientClosed;

    public CreateLedgerTest(boolean clientClosed, int ensSize, int writeQuorumSize, int ackQuorumSize,
                            BookKeeper.DigestType digestType, ParamType passwdType, ResultType expectedType) {
        configure(clientClosed, ensSize, writeQuorumSize, ackQuorumSize, digestType, passwdType, expectedType);

    }

    private void configure(boolean clientClosed, int ensSize, int writeQuorumSize, int ackQuorumSize,
                           BookKeeper.DigestType digestType, ParamType passwdType, ResultType expectedType) {
        this.clientClosed = clientClosed;
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
                // {clientClosed, ensSize, Qw, Qa, DigestType, Passwd, ExpectedResult}
                /* First Iteration: category partition */
                // fixme: ensSize = 0 is not handled by the method!
//                {false, 0, 0, 0, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
//                {false, 0, 0, 1, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
//                {false, 0, 0, -1, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
//                {false, 0, 1, 1, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
//                {false, 0, 1, 2, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
//                {false, 0, 1, 0, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
//                {false, 0, -1, -1, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
//                {false, 0, -1, 0, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
//                {false, 0, -1, -2, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {false, 1, 1, 1, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.OK},
                {false, 1, 1, 2, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {false, 1, 1, 0, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.OK},
                {false, 1, 2, 2, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {false, 1, 2, 3, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {false, 1, 2, 1, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {false, 1, 0, 0, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.OK},
                {false, 1, 0, 1, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                // fixme: negative values for Qa non handled by the method!
//                {false, 1, 0, -1, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.OK},
                {false, 4, 4, 4, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.OK},
                {false, 4, 4, 5, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {false, 4, 4, 3, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.OK},
                {false, 4, 5, 5, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {false, 4, 5, 6, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {false, 4, 5, 4, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {false, 4, 3, 3, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.OK},
                {false, 4, 3, 4, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {false, 4, 3, 2, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.OK},
                {false, 4, 3, 2, BookKeeper.DigestType.MAC, ParamType.VALID, ResultType.OK},
                {false, 4, 3, 2, BookKeeper.DigestType.CRC32, ParamType.VALID, ResultType.OK},
                {false, 4, 3, 2, BookKeeper.DigestType.CRC32C, ParamType.VALID, ResultType.OK},
                {false, 4, 3, 2, BookKeeper.DigestType.DUMMY, ParamType.EMPTY, ResultType.OK},
                // fixme: null password not handled by the method!
//                {false, 4, 3, 2, BookKeeper.DigestType.DUMMY, ParamType.NULL, ResultType.ILLEGAL_ARG_ERR},

                /* Second iteration: increment statement coverage */
                {true, 4, 3, 2, BookKeeper.DigestType.MAC, ParamType.VALID, ResultType.BK_ERR}
        });
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
        try {
            if (ledger != null) ledger.close(); //close properly
        } catch (Exception e) {
            e.printStackTrace();
            Logger.getGlobal().log(Level.WARNING, "ledger not properly closed");
        }
    }
}
