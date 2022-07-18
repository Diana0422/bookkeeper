package org.apache.bookkeeper.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class BookieTest {
    private String filename = "BookieLog";
    private static final byte[] key = "master".getBytes(StandardCharsets.UTF_8);
    private static final long eId = 2147483649L;
    private static long ledId;
    private static final String mess = "message for test";
    private static final BookkeeperInternalCallbacks.WriteCallback wcb = (rc, ledgerId, entryId, addr, ctx) -> {}; // used only when writeDataToJournal=false and the default=true
    private static BookieServer server;

    // getBookieAddress data
    private ServerConfiguration conf;

    // writeEntry data
    private ByteBuf entry; // valid, not valid, null
    private boolean ackBeforeSync; // true, false
    private BookkeeperInternalCallbacks.WriteCallback cb;
    private Object ctx; // valid, not valid (null)
    byte[] masterKey; // valid,  not valid, null
    boolean validEntry;

    // readEntry data
    private long ledgerId; // <0, =0, >0
    private long entryId; // <0, =0, >0

    // checkDirectoryStructure data
    private File dir; // valid (has parent), not valid (without parent)

    // expected result
    private TestExpectations expectation;

    public BookieTest(ServerConfiguration conf, ByteBuf entry, boolean validentry, boolean ackBeforeSync, BookkeeperInternalCallbacks.WriteCallback cb, Object ctx, byte[] masterKey,
                      long ledgerId, long entryId, TestExpectations exp) {
        this.conf = conf;
        this.entry = entry;
        this.validEntry = validentry;
        this.ackBeforeSync = ackBeforeSync;
        this.cb = cb;
        this.ctx = ctx;
        if (validEntry) {
            this.entryId = eId;
            this.ledgerId = ledId;
        } else {
            this.entryId = entryId;
            this.ledgerId = ledgerId;
        }
        this.masterKey = masterKey;
        this.expectation = exp;
    }

    @Parameterized.Parameters
    public static Collection inputs() {
        //Advertized conf
        ServerConfiguration advConf = new ServerConfiguration();
        advConf.setAdvertisedAddress("127.0.0.1");

        // Empty conf
        ServerConfiguration emptyConf = new ServerConfiguration();
        emptyConf.setAdvertisedAddress("");
        emptyConf.setListeningInterface("lo");

        // Null conf
        ServerConfiguration nullConf = new ServerConfiguration();
        nullConf.setAdvertisedAddress(null);

        // Short host name config
        ServerConfiguration shortConf = new ServerConfiguration();
        shortConf.setListeningInterface("lo");
        shortConf.setBookiePort(2181); //zookeeper default port
        shortConf.setUseShortHostName(true);
        shortConf.setUseHostNameAsBookieID(true);
        shortConf.setAllowLoopback(false); // expected false

        // allow loopback config
        ServerConfiguration allowConf = new ServerConfiguration();
        allowConf.setListeningInterface("lo");
        allowConf.setBookiePort(2181); // zookeeper default port
        allowConf.setUseShortHostName(false);
        allowConf.setUseHostNameAsBookieID(true);
        allowConf.setAllowLoopback(true); // expected true

        // Denied loopback config
        ServerConfiguration denyConf = new ServerConfiguration();
        denyConf.setAdvertisedAddress("127.0.0.1");
        denyConf.setListeningInterface("");
        denyConf.setAllowLoopback(false);

//        try {
//            server = new BookieServer(allowConf);
//            server.start();
//        } catch (InterruptedException | IOException | KeeperException | BookieException | ReplicationException.UnavailableException | ReplicationException.CompatibilityException | SecurityException e) {
//            e.printStackTrace();
//        }

        TestExpectations failTest1 = new TestExpectations(false, true, true, true);
        TestExpectations failTest2 = new TestExpectations(true, false, true, true);
        TestExpectations failTest3 = new TestExpectations(true, true, false, true);
        TestExpectations failTest4 = new TestExpectations(true, true, false, false);
        TestExpectations failTests13 = new TestExpectations(false, true, false, true);
        TestExpectations failTests23 = new TestExpectations(true, false, false, true);
        TestExpectations noFail = new TestExpectations(true, true, true, true);

        return Arrays.asList(new Object[][] {
                //{new ServerConfiguration(), Unpooled.buffer(), true, false, wcb, "this_is_context", "master".getBytes(StandardCharsets.UTF_8), 0, 0, failTest3},
                //{advConf, Unpooled.buffer(), true, false, wcb, null, "master".getBytes(StandardCharsets.UTF_8), ledId, 0, failTest3},
                //{emptyConf, Unpooled.buffer(), true, false, wcb, "this_is_context", "master".getBytes(StandardCharsets.UTF_8), 0, 0, failTests13},
                //{shortConf, Unpooled.buffer(), true, false, wcb, "this_is_context", "master".getBytes(StandardCharsets.UTF_8), 0, 0, failTest3},
                //{denyConf, Unpooled.buffer(), true, false, wcb, "this_is_context", "master".getBytes(StandardCharsets.UTF_8), 0, 0, failTest3},
                //{allowConf, Unpooled.buffer(), true, false, wcb, "this_is_context", "master".getBytes(StandardCharsets.UTF_8), 0, 0, failTest3},
                //{advConf, Unpooled.buffer(), false, true, wcb, "this_is_context", "master".getBytes(StandardCharsets.UTF_8), 0, 0, failTests23},
                //{advConf, Unpooled.buffer(), false, true, wcb, "this_is_context", "master".getBytes(StandardCharsets.UTF_8), ledId, eId, failTests23},
                //{advConf, Unpooled.buffer(), true, false, wcb, "this_is_context", "master".getBytes(StandardCharsets.UTF_8), 0, 0, failTest3},
                //{advConf, Unpooled.buffer(), true, true, null, "this_is_context", "master".getBytes(StandardCharsets.UTF_8), 0, 0, failTest3},
                //{advConf, Unpooled.buffer(), true, true, wcb, "this_is_context", "".getBytes(StandardCharsets.UTF_8), 0, 0, failTest3},
                //{advConf, Unpooled.buffer(), true, true, wcb, "this_is_context", "master".getBytes(StandardCharsets.UTF_8), 0, 0, failTest3},
                //{advConf, Unpooled.buffer(), true, true, wcb, "this_is_context", "master".getBytes(StandardCharsets.UTF_8), -1, 0, failTest3},
                //{advConf, Unpooled.buffer(), true, true, wcb, "this_is_context", "master".getBytes(StandardCharsets.UTF_8), 0, -1, failTest3},
                //{advConf, Unpooled.buffer(), true, true, wcb, "this_is_context", "master".getBytes(StandardCharsets.UTF_8), 1, 0, failTest3},
                {advConf, Unpooled.buffer(), true, true, wcb, "this_is_context", "master".getBytes(StandardCharsets.UTF_8), 0, 1, failTest3}
        });
    }

    @Test
    public void provaTest() {
        assertEquals(true, true);
    }
}
