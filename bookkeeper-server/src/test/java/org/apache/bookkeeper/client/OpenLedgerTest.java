package org.apache.bookkeeper.client;

import org.apache.bookkeeper.utils.ParamType;
import org.apache.bookkeeper.utils.ResultType;
import org.junit.Assert;
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

    public OpenLedgerTest(long ledgerId, BookKeeper.DigestType digestType, ParamType passwdType, ResultType resultType) {
        configure(ledgerId, digestType, passwdType, resultType);
    }

    private void configure(long ledgerId, BookKeeper.DigestType digestType, ParamType passwdType, ResultType resultType) {
        configureDigestType(digestType);
        configureOpeningPasswd(passwdType);
        configureResult(resultType);
        this.ledgerId = ledgerId;
        this.ensSize = 1;
        this.writeQuorumSize = 1;
        this.ackQuorumSize = 1;
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
                // {lId, DigestType, passwd, result}
                {4113L, BookKeeper.DigestType.DUMMY, ParamType.CORRECT, ResultType.OK},
                {2001L, BookKeeper.DigestType.DUMMY, ParamType.CORRECT, ResultType.BK_ERR},
                {4113L, BookKeeper.DigestType.CRC32, ParamType.CORRECT, ResultType.OK},
                {4113L, BookKeeper.DigestType.MAC, ParamType.CORRECT, ResultType.OK},
                {4113L, BookKeeper.DigestType.CRC32C, ParamType.CORRECT, ResultType.OK},
                {4113L, BookKeeper.DigestType.DUMMY, ParamType.EMPTY, ResultType.OK},
                {4113L, BookKeeper.DigestType.DUMMY, ParamType.INCORRECT, ResultType.ILLEGAL_ARG_ERR},
//                {-1, BookKeeper.DigestType.DUMMY, ParamType.CORRECT, ResultType.BK_ERR}
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
    public void openTest() {
        LedgerHandle handle = null;
        if (ledgerId == 4113L && passType == ParamType.CORRECT) {
            try {
                handle = bk.createLedger(ensSize, writeQuorumSize, ackQuorumSize, digestType, passwd);
                handle.close();
                assertTrue(handle.isClosed());
                if (handle.isClosed()) Logger.getGlobal().log(Level.WARNING, "ledger closed.");
            } catch (InterruptedException | BKException e) {
                e.printStackTrace();
            }
        }

        if (handle == null){
            // ledger does not exists
            try {
                handle = bk.openLedger(ledgerId, digestType, passwd);
                assertNotNull(handle);
            }catch (BKException | InterruptedException e){
                assertEquals(expectedError.getClass(), e.getClass());
            }
        } else {
            // ledger exists and was previously closed
            try {
                handle = bk.openLedger(ledgerId, digestType, passwd);
                assertNotNull(handle);
            } catch (BKException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
