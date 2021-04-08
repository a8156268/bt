package bt.torrent.messaging;

import bt.net.ConnectionKey;
import bt.processor.torrent.TorrentContext;
import bt.runtime.Config;
import bt.torrent.selector.PieceSelector;

/**
 * @author luoml
 * @date 2021/3/1
 */
public interface IAssignmentFactory {

    Assignments createAssignments(TorrentContext torrentContext, PieceSelector selector, Config config);

    Assignment createAssignment(TorrentContext torrentContext, PieceSelector selector, Config config, ConnectionKey connectionKey, Assignments assignments);
}
