package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookieClientImpl;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.bookkeeper.utils.ExpectedLedger;
import org.apache.bookkeeper.utils.ResultType;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.Before;
import org.mockito.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public abstract class BookKeeperTest {

    @Mock
    private BookieWatcher bookieWatcher;
    @Mock
    private LedgerIdGenerator ledgerIdGenerator;
    @Mock
    protected LedgerManager ledgerManager;
    @Mock
    private CompletableFuture<Versioned<LedgerMetadata>> whenComplete;
    @Mock
    protected CompletableFuture<Versioned<LedgerMetadata>> whenCompleteCloseOk;
    @Mock
    protected CompletableFuture<Versioned<LedgerMetadata>> whenCompleteCloseWrong;
    @Mock
    protected CompletableFuture<Void> whenCompleteRemoveOk;
    @Mock
    protected CompletableFuture<Void> whenCompleteRemoveWrong;
    @Mock
    private Versioned<LedgerMetadata> versioned;
    @Mock
    protected LedgerMetadata metadata;
    @Mock
    private Void voidDelete;
    private OrderedExecutor executor;
    @Spy
    protected BookKeeper bk;

    private Map<String, MockedStatic<?>> mockedStatics = new HashMap<>();

    protected int ensSize;
    protected int writeQuorumSize;
    protected int ackQuorumSize;
    protected BookKeeper.DigestType digestType;
    protected byte[] passwd;

    protected ExpectedLedger expected;
    protected Exception expectedError;

    public BookKeeperTest() {
        MockitoAnnotations.initMocks(this);
        executor = OrderedExecutor.newBuilder().build();
    }

    @Before
    public void setUp() throws Exception {
        System.out.println("Expected:" + expected);
        doAnswer(invocation -> {
            ((BookkeeperInternalCallbacks.GenericCallback<Long>) invocation.getArguments()[0]).operationComplete(BKException.Code.OK, new Long(4113));
            return null;
        }).when(ledgerIdGenerator).generateLedgerId(any(BookkeeperInternalCallbacks.GenericCallback.class));

        // mocking dependencies to execute ledger creation
        when(bk.getBookieWatcher()).thenReturn(bookieWatcher);
        doAnswer(invocationOnMock -> {
            ArrayList<BookieId> bookies = new ArrayList<>();
            for (int i = 0; i < ensSize; i++) {
                BookieId bookie = BookieId.parse("BookieNo" + i);
                bookies.add(bookie);
            }
            return bookies;
        }).when(bookieWatcher).newEnsemble(anyInt(), anyInt(), anyInt(), any());

        when(bk.getLedgerIdGenerator()).thenReturn(ledgerIdGenerator);
        when(bk.getLedgerManager()).thenReturn(ledgerManager);
        when(ledgerManager.createLedgerMetadata(anyLong(), any(LedgerMetadata.class))).thenReturn(whenComplete);
        when(versioned.getValue()).thenReturn(metadata);
        doAnswer( invocation -> {
            ((BiConsumer<Versioned<LedgerMetadata>, Throwable>) invocation.getArguments()[0]).accept(versioned, null);
            return null;
        }).when(whenComplete).whenComplete(any(BiConsumer.class));

        // to close the ledger
        when(metadata.getAllEnsembles()).thenAnswer(invocationOnMock -> {
            TreeMap<Long, List<BookieId>> tree = new TreeMap<>();
            ArrayList<BookieId> bookies = new ArrayList<>();
            for (int i = 0; i < ensSize; i++) {
                BookieId bookie = BookieId.parse("BookieNo" + i);
                bookies.add(bookie);
            }
            // add ensemble only id ensSize > 0
            if (ensSize > 0) tree.putIfAbsent(1L, bookies);
            return tree;
        });
        when(bk.getClientCtx().getMainWorkerPool()).thenReturn(executor);
        if (ensSize > 0) {
            LedgerMetadata meta = LedgerMetadataBuilder.from(metadata)
                    .withClosedState().withLastEntryId(-1)
                    .withLength(0).build();
            Versioned<LedgerMetadata> versionedMeta = new Versioned<>(meta, Version.ANY);
            when(ledgerManager.writeLedgerMetadata(anyLong(), any(), any())).thenReturn(whenCompleteCloseOk);
            doAnswer( invocation -> {
                ((BiConsumer<Versioned<LedgerMetadata>, Throwable>) invocation.getArguments()[0]).accept(versionedMeta, null);
                return null;
            }).when(whenCompleteCloseOk).whenComplete(any(BiConsumer.class));
            doAnswer( invocation -> {
                ((BiConsumer<Versioned<LedgerMetadata>, Throwable>) invocation.getArguments()[0]).accept(versionedMeta, new BKException.BKNoSuchLedgerExistsOnMetadataServerException());
                return null;
            }).when(whenCompleteCloseWrong).whenComplete(any(BiConsumer.class));
        }

        // delete ledger
        doAnswer( invocation -> {
            ((BiConsumer<Void, Throwable>) invocation.getArguments()[0]).accept(voidDelete, null);
            return null;
        }).when(whenCompleteRemoveOk).whenCompleteAsync(any(BiConsumer.class), any());
        doAnswer( invocation -> {
            ((BiConsumer<Void, Throwable>) invocation.getArguments()[0]).accept(voidDelete, new BKException.BKNoSuchLedgerExistsOnMetadataServerException());
            return null;
        }).when(whenCompleteRemoveWrong).whenCompleteAsync(any(BiConsumer.class), any());
    }


    protected void configureQuorum(int ensSize, int writeQuorumSize, int ackQuorumSize) {
        this.ensSize = ensSize;
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;
    }

    protected void configureDigestType(BookKeeper.DigestType digestType) {
        this.digestType = digestType;
    }

    protected void configureResult(ResultType resultType) {
        ExpectedLedger ledger;
        switch (resultType) {
            case OK:
                ledger = new ExpectedLedger(4113L);
                this.expected = ledger;
                break;
            case NULL_PTR_ERR:
                this.expectedError = new NullPointerException();
                break;
            case ILLEGAL_ARG_ERR:
                this.expectedError = new IllegalArgumentException();
                break;
            case UNEXPECTED_ERR:
                this.expectedError = new BKException.BKUnexpectedConditionException();
                break;
            case BK_ERR:
                this.expectedError = new BKException.BKNoSuchLedgerExistsOnMetadataServerException();
                break;
            default:
                break;
        }
    }
}
