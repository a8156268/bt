package bt.torrent.messaging;

import bt.net.ConnectionKey;

import java.util.Optional;
import java.util.Set;

/**
 * @author luoml
 * @date 2021/3/1
 */
public interface Assignments {

    int count();

    Assignment get(ConnectionKey disconnectedPeer);

    Optional<Assignment> assign(ConnectionKey connectionKey);

    void remove(Assignment assignment);

    boolean claim(int pieceIndex);

    void finish(int pieceIndex);

    boolean isEndgame();

    Set<ConnectionKey> update(Set<ConnectionKey> ready, Set<ConnectionKey> choking);

}
