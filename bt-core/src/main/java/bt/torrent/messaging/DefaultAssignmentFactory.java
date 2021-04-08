package bt.torrent.messaging;


import bt.net.ConnectionKey;
import bt.processor.torrent.TorrentContext;
import bt.runtime.Config;
import bt.torrent.selector.PieceSelector;

/**
 * @author luoml
 * @date 2021/3/2
 */
public class DefaultAssignmentFactory implements IAssignmentFactory {

    @Override
    public Assignments createAssignments(TorrentContext torrentContext, PieceSelector selector, Config config) {
        return new DefaultAssignments(torrentContext, selector, config, this);
    }

    @Override
    public Assignment createAssignment(TorrentContext torrentContext, PieceSelector selector, Config config, ConnectionKey connectionKey, Assignments assignments) {
        return new DefaultAssignment(connectionKey, config.getMaxPieceReceivingTime(), selector, torrentContext.getPieceStatistics(),
                assignments, config.getMaxSimultaneouslyAssignedPieces());
    }
}
