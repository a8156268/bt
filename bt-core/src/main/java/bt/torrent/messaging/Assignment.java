package bt.torrent.messaging;

import bt.net.ConnectionKey;

import java.util.Collection;

/**
 * @author luoml
 * @date 2021/3/1
 */
public interface Assignment {

    enum Status {ACTIVE, TIMEOUT}

    boolean isAssigned(int pieceIndex);

    void finish(int pieceIndex);

    void check();

    void abort();

    void start(ConnectionState connectionState);

    ConnectionKey getConnectionKey();

    Assignment.Status getStatus();

    Collection<Integer> getPieces();

}
