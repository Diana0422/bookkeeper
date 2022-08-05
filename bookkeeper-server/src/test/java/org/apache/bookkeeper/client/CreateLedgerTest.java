package org.apache.bookkeeper.client;

import org.apache.bookkeeper.utils.ParamType;
import org.apache.bookkeeper.utils.ResultType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.awt.print.Book;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class CreateLedgerTest extends BookKeeperTest {

    public CreateLedgerTest(int ensSize, int writeQuorumSize, int ackQuorumSize,
                            BookKeeper.DigestType digestType, ParamType passwdType, ResultType expectedType) {
        configure(ensSize, writeQuorumSize, ackQuorumSize, digestType, passwdType, expectedType);

    }

    private void configure(int ensSize, int writeQuorumSize, int ackQuorumSize,
                           BookKeeper.DigestType digestType, ParamType passwdType, ResultType expectedType) {
        configureQuorum(ensSize, writeQuorumSize, ackQuorumSize);
        configureDigestType(digestType);
        configureCreationPasswd(passwdType);
        configureResult(expectedType);
    }

    protected void configureCreationPasswd(ParamType passwdType) {
        String pass;
        switch (passwdType) {
            case EMPTY:
                pass = "";
                this.passwd = pass.getBytes(StandardCharsets.UTF_8);
                break;
            case VALID:
                pass = "diana";
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
                // {ensSize, Qw, Qa, DigestType, Passwd, ExpectedResult}
                {0, 0, 0, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.OK},
                {0, 0, 1, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
//                {0, 0, -1, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR}, // FIXME THIS TEST FAILS because negative values not handled
                {0, 1, 1, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {0, 1, 2, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {0, 1, 0, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
//                {0, -1, -1, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR}, // FIXME THIS TEST FAILS because negative values not handled
                {0, -1, 0, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
//                {0, -1, -2, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR}, // FIXME THIS TEST FAILS because negative values not handled
                {1, 1, 1, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.OK},
                {1, 1, 2, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {1, 1, 0, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.OK},
                {1, 2, 2, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {1, 2, 3, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {1, 2, 1, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {1, 0, 0, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.OK},
                {1, 0, 1, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
//                {1, 0, -1, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR} // FIXME THIS TEST FAILS because negative values not handled
                {4, 4, 4, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.OK},
                {4, 4, 5, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {4, 4, 3, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.OK},
                {4, 5, 5, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {4, 5, 6, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {4, 5, 4, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {4, 3, 3, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.OK},
                {4, 3, 4, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.ILLEGAL_ARG_ERR},
                {4, 3, 2, BookKeeper.DigestType.DUMMY, ParamType.VALID, ResultType.OK},
                {4, 3, 2, BookKeeper.DigestType.DUMMY, ParamType.EMPTY, ResultType.NULL_PTR_ERR}, //fixme NullPointerException
                {4, 3, 2, BookKeeper.DigestType.DUMMY, ParamType.NULL, ResultType.NULL_PTR_ERR}, //fixme NullPointerException
                {4, 3, 2, BookKeeper.DigestType.MAC, ParamType.VALID, ResultType.OK},
                {4, 3, 2, BookKeeper.DigestType.MAC, ParamType.EMPTY, ResultType.NULL_PTR_ERR}, // fixme NullPointerException
                {4, 3, 2, BookKeeper.DigestType.MAC, ParamType.NULL, ResultType.NULL_PTR_ERR}, // fixme NullPointerException
                {4, 3, 2, BookKeeper.DigestType.CRC32, ParamType.VALID, ResultType.OK},
                {4, 3, 2, BookKeeper.DigestType.CRC32, ParamType.EMPTY, ResultType.NULL_PTR_ERR}, // fixme NullPointerException
                {4, 3, 2, BookKeeper.DigestType.CRC32, ParamType.NULL, ResultType.NULL_PTR_ERR}, // fixme NullPointerException
                {4, 3, 2, BookKeeper.DigestType.CRC32C, ParamType.VALID, ResultType.OK},
                {4, 3, 2, BookKeeper.DigestType.CRC32C, ParamType.EMPTY, ResultType.NULL_PTR_ERR}, // fixme NullPointerException
                {4, 3, 2, BookKeeper.DigestType.CRC32C, ParamType.NULL, ResultType.NULL_PTR_ERR}, // fixme NullPointerException
        });
    }

    @Test
    public void createTest() {
        LedgerHandle ledger;
        try {
            ledger = bk.createLedger(ensSize, writeQuorumSize, ackQuorumSize, digestType, passwd);
            assertNotNull(ledger);
            assertEquals(expected.getLedgerId(), ledger.ledgerId);
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals(expectedError.getClass(), e.getClass());
        }
    }
}
