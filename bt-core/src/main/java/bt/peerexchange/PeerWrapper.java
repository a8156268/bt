package bt.peerexchange;

import bt.net.Peer;

import java.util.Objects;

/**
 * Provide equals and hashCode for {@link Peer}
 */
public class PeerWrapper {

    private final Peer peer;

    public PeerWrapper(Peer peer) {
        this.peer = peer;
    }

    public Peer getPeer() {
        return peer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PeerWrapper)) return false;
        PeerWrapper that = (PeerWrapper) o;
        return Objects.equals(peer.getInetAddress(), that.peer.getInetAddress())
                && peer.getPort() == this.peer.getPort();
    }

    @Override
    public int hashCode() {
        return Objects.hash(peer.getInetAddress(), peer.getPort());
    }

    public static boolean equals(Peer left, Peer right) {
        return Objects.equals(left.getInetAddress(), right.getInetAddress())
                && left.getPort() == right.getPort();
    }
}
