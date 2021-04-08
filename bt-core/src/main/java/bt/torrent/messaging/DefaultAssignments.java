/*
 * Copyright (c) 2016—2017 Andrei Tomashpolskiy and individual contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bt.torrent.messaging;

import bt.data.Bitfield;
import bt.net.ConnectionKey;
import bt.processor.torrent.TorrentContext;
import bt.runtime.Config;
import bt.torrent.BitfieldBasedStatistics;
import bt.torrent.selector.PieceSelector;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultAssignments implements Assignments {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultAssignments.class);

    private TorrentContext torrentContext;
    private Config config;

    private Bitfield bitfield;
    private PieceSelector selector;
    private BitfieldBasedStatistics pieceStatistics;

    private IntSet assignedPieces;
    private Map<ConnectionKey, Assignment> assignments;

    private IAssignmentFactory assignmentFactory;

    private volatile boolean endgame = false;

    public DefaultAssignments(TorrentContext torrentContext, PieceSelector selector, Config config, IAssignmentFactory assignmentFactory) {
        this.torrentContext = torrentContext;
        this.bitfield = torrentContext.getBitfield();
        this.selector = selector;
        this.pieceStatistics = torrentContext.getPieceStatistics();
        this.config = config;

        this.assignedPieces = IntSets.synchronize(new IntOpenHashSet());
        this.assignments = new ConcurrentHashMap<>();

        this.assignmentFactory = assignmentFactory;
    }

    public Assignment get(ConnectionKey connectionKey) {
        return assignments.get(connectionKey);
    }

    public Map<ConnectionKey, Assignment> getAssignments() {
        return assignments;
    }

    public void remove(Assignment assignment) {
        assignment.abort();
        assignments.remove(assignment.getConnectionKey());
        // TODO: investigate on how this might affect endgame?
        LOGGER.info("remove assignedPieces, count: {}", assignment.getPieces().size());
        assignedPieces.removeAll(assignment.getPieces());
        assignment.getPieces().clear();
    }

    public int count() {
        return assignments.size();
    }

    public Optional<Assignment> assign(ConnectionKey connectionKey) {
        if (!hasInterestingPieces(connectionKey)) {
            LOGGER.info("assign assignment for failed: {}, not Interesting", connectionKey);
            return Optional.empty();
        }

        Assignment assignment = assignmentFactory.createAssignment(torrentContext, selector, config, connectionKey, this);
        assignments.put(connectionKey, assignment);
        return Optional.of(assignment);
    }

    public boolean claim(int pieceIndex) {
        if (bitfield.isComplete(pieceIndex)) {
            return false;
        }
        if (endgame) {
            return true;
        }
        if (assignedPieces.add(pieceIndex)) {
            return true;
        }
        return isEndgame();
    }

    public void finish(int pieceIndex) {
        assignedPieces.remove(pieceIndex);
    }

    public boolean isEndgame() {
        // if all remaining pieces are requested,
        // that would mean that we have entered the "endgame" mode
        if (endgame) {
            return true;
        }
        if (bitfield.getPiecesRemaining() <= assignedPieces.size()) {
            endgame = true;
        }
        return endgame;
    }

    /**
     * @return Collection of peers that have interesting pieces and can be given an assignment
     */
    public Set<ConnectionKey> update(Set<ConnectionKey> ready, Set<ConnectionKey> choking) {
        Set<ConnectionKey> result = new HashSet<>();
        for (ConnectionKey peer : ready) {
            if (hasInterestingPieces(peer)) {
                result.add(peer);
            }
        }
        for (ConnectionKey peer : choking) {
            if (hasInterestingPieces(peer)) {
                result.add(peer);
            }
        }

        return result;
    }

    private boolean hasInterestingPieces(ConnectionKey connectionKey) {
        Optional<Bitfield> peerBitfieldOptional = pieceStatistics.getPeerBitfield(connectionKey);
        if (!peerBitfieldOptional.isPresent()) {
            return false;
        }
        Bitfield peerBitfield = peerBitfieldOptional.get();
        Bitfield bitfield = this.getBitfield();
        for (int i = 0; i < bitfield.getPiecesTotal(); ++i) {
            if (!bitfield.isSet(i) && peerBitfield.isSet(i)) {
                return true;
            }
        }
        return false;
    }

    public Bitfield getBitfield() {
        return bitfield;
    }
}
