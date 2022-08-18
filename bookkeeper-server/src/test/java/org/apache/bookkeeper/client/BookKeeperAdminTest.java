package org.apache.bookkeeper.client;

import org.apache.bookkeeper.stats.StatsLogger;
import org.mockito.*;

import java.util.Enumeration;

public abstract class BookKeeperAdminTest {

    @Spy
    @InjectMocks
    protected BookKeeperAdmin bookKeeperAdmin;

    // mocks
    @Spy protected BookKeeper bkc;
    @Mock protected StatsLogger statsLogger;
    @Mock protected LedgerHandle ledgerHandle;
    Enumeration<LedgerEntry> entries;
    protected MockedStatic<SyncCallbackUtils> syncCallbackUtils;

    public BookKeeperAdminTest() {
        MockitoAnnotations.initMocks(this);
    }
}
