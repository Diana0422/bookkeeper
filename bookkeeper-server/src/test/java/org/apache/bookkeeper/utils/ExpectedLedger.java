package org.apache.bookkeeper.utils;

import lombok.Data;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.proto.checksum.DigestManager;

import java.security.NoSuchAlgorithmException;

@Data
public class ExpectedLedger {
    private long ledgerId;

    public ExpectedLedger(long ledgerId) {
        this.ledgerId = ledgerId;
    }
}
