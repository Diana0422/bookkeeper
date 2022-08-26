package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.utils.ConfType;
import org.apache.bookkeeper.utils.ResultType;
import org.apache.kerby.config.Conf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class BookieSocketAddressTest extends BookieTest {

    private Bookie bookie;
    private ServerConfiguration conf;

    private String address;

    public BookieSocketAddressTest(ConfType confType, String resultType) {
        MockitoAnnotations.initMocks(this);
        configure(confType, resultType);
    }

    private void configure(ConfType confType, String resultType) {
        address = resultType;

        conf = new ServerConfiguration();
        switch (confType) {
            case EMPTY:
                conf.setAdvertisedAddress("");
                conf.setListeningInterface("lo");
                break;
            case NULL:
                conf.setAdvertisedAddress(null);
                break;
            case ADV:
                conf.setAdvertisedAddress("127.0.0.1");
                break;
            case ALLOW_LOOPBACK:
                conf.setListeningInterface("lo");
                conf.setBookiePort(2181); // zookeeper default port
                conf.setUseShortHostName(false);
                conf.setUseHostNameAsBookieID(true);
                conf.setAllowLoopback(true); // expected true
                break;
            case DENIED_LOOPBACK:
                conf.setAdvertisedAddress("127.0.0.1");
                conf.setListeningInterface("");
                conf.setAllowLoopback(false);
                break;
            default:
                break;
        }

        try {
            bookie = new Bookie(conf);
            bookie.start();
        } catch (IOException | InterruptedException | BookieException e) {
            e.printStackTrace();
        }

    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
//                {ConfType.ADV, "127.0.0.1"},
//                {ConfType.EMPTY, "192.168.56.1"},
//                {ConfType.ALLOW_LOOPBACK, "LAPTOP-FT4B5NTB"},
//                {ConfType.DENIED_LOOPBACK, "127.0.0.1"},
//                {ConfType.NULL, "192.168.56.1"}
        });
    }


    @Test
    public void test() {
//        try {
//            BookieSocketAddress bookieAddress = Bookie.getBookieAddress(conf);
//            assertEquals(address, bookieAddress.getHostName());
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
    }
}
