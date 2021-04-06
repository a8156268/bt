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

package bt.data;

import bt.BtException;
import bt.protocol.BitOrder;
import bt.protocol.Protocols;

import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Status of torrent's data.
 * <p>
 * Instances of this class are thread-safe.
 *
 * @since 1.0
 */
public class Bitfield {

    // TODO: use EMPTY and PARTIAL instead of INCOMPLETE

    /**
     * Status of a particular piece.
     *
     * @since 1.0
     */
    public enum PieceStatus {
        /*EMPTY, PARTIAL,*/INCOMPLETE, COMPLETE, COMPLETE_VERIFIED
    }

    private static final int BIT_COUNT = 6;

    private static final int SEGMENT_COUNT = 1 << BIT_COUNT;

    private static final int MASK = SEGMENT_COUNT - 1;

    /**
     * Bitmask indicating availability of pieces.
     * If the x-th bit of y-th {@link Segment#bitSet} is set, then the n-th piece is complete and verified.
     * If the x-th bit of y-th {@link Segment#skipped} is set, then the n-th piece should be skipped.
     * <p>
     * x is computed by {@link Bitfield#bitSetPosition(int)}
     * y is computed by {@link Bitfield#bitSetIndex(int)}
     */
    private final Segment[] segments = new Segment[SEGMENT_COUNT];

    /**
     * Total number of pieces in torrent.
     */
    private final int piecesTotal;

    private final AtomicInteger pieceCompleted = new AtomicInteger(0);

    private final AtomicInteger pieceSkipped = new AtomicInteger(0);

    /**
     * List of torrent's chunk descriptors.
     * Absent when this Bitfield instance is describing data that some peer has.
     */
    private final Optional<List<ChunkDescriptor>> chunks;

    /**
     * Creates "local" bitfield from a list of chunk descriptors.
     *
     * @param chunks List of torrent's chunk descriptors.
     * @since 1.0
     */
    public Bitfield(List<ChunkDescriptor> chunks) {
        this.piecesTotal = chunks.size();
        initBitMask(this.segments, piecesTotal);
        this.chunks = Optional.of(chunks);
    }

    /**
     * Creates empty bitfield.
     * Useful when peer does not communicate its' bitfield (e.g. when he has no data).
     *
     * @param piecesTotal Total number of pieces in torrent.
     * @since 1.0
     */
    public Bitfield(int piecesTotal) {
        this.piecesTotal = piecesTotal;
        initBitMask(this.segments, piecesTotal);
        this.chunks = Optional.empty();
    }

    private void initBitMask(Segment[] segments, int piecesTotal) {
        int segmentSize = (piecesTotal + SEGMENT_COUNT - 1) / SEGMENT_COUNT;
        for (int i = 0; i < SEGMENT_COUNT; ++i) {
            segments[i] = new Segment(segmentSize);
        }
    }

    /**
     * Creates bitfield based on a bitmask.
     * Used for creating peers' bitfields.
     * <p>
     * Bitmask must be in the format described in BEP-3 (little-endian order of bits).
     *
     * @param value       Bitmask that describes status of all pieces.
     *                    If position i is set to 1, then piece with index i is complete and verified.
     * @param piecesTotal Total number of pieces in torrent.
     * @since 1.0
     * @deprecated since 1.7 in favor of {@link #Bitfield(byte[], BitOrder, int)}
     */
    @Deprecated
    public Bitfield(byte[] value, int piecesTotal) {
        this(value, BitOrder.LITTLE_ENDIAN, piecesTotal);
    }

    /**
     * Creates bitfield based on a bitmask.
     * Used for creating peers' bitfields.
     *
     * @param value       Bitmask that describes status of all pieces.
     *                    If position i is set to 1, then piece with index i is complete and verified.
     * @param piecesTotal Total number of pieces in torrent.
     * @since 1.7
     */
    public Bitfield(byte[] value, BitOrder bitOrder, int piecesTotal) {
        this.piecesTotal = piecesTotal;
        initBitMask(this.segments, piecesTotal);
        initBits(value, bitOrder, piecesTotal);
        this.chunks = Optional.empty();
    }

    private void initBits(byte[] bytes, BitOrder bitOrder, int piecesTotal) {
        int expectedBitmaskLength = getBitmaskLength(piecesTotal);
        if (bytes.length != expectedBitmaskLength) {
            throw new IllegalArgumentException("Invalid bitfield: total (" + piecesTotal +
                    "), bitmask length (" + bytes.length + "). Expected bitmask length: " + expectedBitmaskLength);
        }

        if (bitOrder == BitOrder.LITTLE_ENDIAN) {
            bytes = Protocols.reverseBits(bytes);
        }

        for (int i = 0; i < piecesTotal; i++) {
            if (Protocols.isSet(bytes, BitOrder.BIG_ENDIAN, i)) {
                setBit(i);
            }
        }
    }

    private static int getBitmaskLength(int piecesTotal) {
        return (int) Math.ceil(piecesTotal / 8d);
    }

    /**
     * @param bitOrder Order of bits to use to create the byte array
     * @return Bitmask that describes status of all pieces.
     * If the n-th bit is set, then the n-th piece
     * is in {@link PieceStatus#COMPLETE_VERIFIED} status.
     * @since 1.7
     */
    public byte[] toByteArray(BitOrder bitOrder) {
        byte[] bytes = new byte[getBitmaskLength(piecesTotal)];
        for (int i = 0; i < piecesTotal; ++i) {
            if (isSet(i)) {
                Protocols.setBit(bytes, bitOrder, i);
            }
        }
        return bytes;

    }

    /**
     * @return Total number of pieces in torrent.
     * @since 1.0
     */
    public int getPiecesTotal() {
        return piecesTotal;
    }

    /**
     * @return Number of pieces that have status {@link PieceStatus#COMPLETE_VERIFIED}.
     * @since 1.0
     */
    public int getPiecesComplete() {
        return pieceCompleted.get();
    }

    /**
     * @return Number of pieces that have status different {@link PieceStatus#COMPLETE_VERIFIED}.
     * @since 1.7
     */
    public int getPiecesIncomplete() {
        return piecesTotal - pieceCompleted.get();
    }

    /**
     * @return Number of pieces that have status different from {@link PieceStatus#COMPLETE_VERIFIED}
     * and should NOT be skipped.
     * @since 1.0
     */
    public int getPiecesRemaining() {
        return getPiecesIncomplete() - getPiecesSkipped();
    }

    /**
     * @return Number of pieces that should be skipped
     * @since 1.7
     */
    public int getPiecesSkipped() {
        return pieceSkipped.get();
    }

    /**
     * @return Number of pieces that should NOT be skipped
     * @since 1.7
     */
    public int getPiecesNotSkipped() {
        return piecesTotal - getPiecesSkipped();
    }

    /**
     * @param pieceIndex Piece index (0-based)
     * @return Status of the corresponding piece.
     * @see DataDescriptor#getChunkDescriptors()
     * @since 1.0
     */
    public PieceStatus getPieceStatus(int pieceIndex) {
        validatePieceIndex(pieceIndex);

        PieceStatus status;

        if (isSet(pieceIndex)) {
            status = PieceStatus.COMPLETE_VERIFIED;
        } else if (chunks.isPresent()) {
            ChunkDescriptor chunk = chunks.get().get(pieceIndex);
            if (chunk.isComplete()) {
                status = PieceStatus.COMPLETE;
            } else {
                status = PieceStatus.INCOMPLETE;
            }
        } else {
            status = PieceStatus.INCOMPLETE;
        }

        return status;
    }


    /**
     * check if piece status is completed and verified
     *
     * @param pieceIndex Piece index (0-based)
     * @return true if piece is completed and verified
     * false if piece is not completed nor verified
     * @since 1.10
     */
    public boolean isSet(int pieceIndex) {
        validatePieceIndex(pieceIndex);

        Segment segment = segments[bitSetIndex(pieceIndex)];
        int position = bitSetPosition(pieceIndex);
        synchronized (segment) {
            return segment.bitSet.get(position);
        }
    }

    /**
     * set piece status to be completed and verified
     *
     * @param pieceIndex Piece index (0-based)
     * @return true if piece is succeed set to be completed and verified
     * false if piece is already completed and verified
     * @since 1.10
     */
    public boolean setBit(int pieceIndex) {
        validatePieceIndex(pieceIndex);

        Segment segment = segments[bitSetIndex(pieceIndex)];
        int position = bitSetPosition(pieceIndex);
        synchronized (segment) {
            if (!segment.bitSet.get(position)) {
                segment.bitSet.set(position);
                if (segment.skipped == null || !segment.skipped.get(position)) {
                    pieceCompleted.incrementAndGet();
                }
                return true;
            }
            return false;
        }
    }

    /**
     * Shortcut method to find out if the piece has been downloaded.
     *
     * @param pieceIndex Piece index (0-based)
     * @return true if the piece has been downloaded
     * @since 1.1
     */
    public boolean isComplete(int pieceIndex) {
        PieceStatus pieceStatus = getPieceStatus(pieceIndex);
        return (pieceStatus == PieceStatus.COMPLETE || pieceStatus == PieceStatus.COMPLETE_VERIFIED);
    }

    /**
     * Shortcut method to find out if the piece has been downloaded and verified.
     *
     * @param pieceIndex Piece index (0-based)
     * @return true if the piece has been downloaded and verified
     * @since 1.1
     */
    public boolean isVerified(int pieceIndex) {
        return isSet(pieceIndex);
    }

    /**
     * Mark piece as complete and verified.
     *
     * @param pieceIndex Piece index (0-based)
     * @see DataDescriptor#getChunkDescriptors()
     * @since 1.0
     */
    public boolean markVerified(int pieceIndex) {
        assertChunkComplete(pieceIndex);
        return setBit(pieceIndex);
    }

    private void assertChunkComplete(int pieceIndex) {
        validatePieceIndex(pieceIndex);

        if (chunks.isPresent()) {
            if (!chunks.get().get(pieceIndex).isComplete()) {
                throw new IllegalStateException("Chunk is not complete: " + pieceIndex);
            }
        }
    }

    private void validatePieceIndex(Integer pieceIndex) {
        if (pieceIndex < 0 || pieceIndex >= getPiecesTotal()) {
            throw new BtException("Illegal piece index: " + pieceIndex +
                    ", expected 0.." + (getPiecesTotal() - 1));
        }
    }

    /**
     * Mark a piece as skipped
     *
     * @since 1.7
     */
    public void skip(int pieceIndex) {
        validatePieceIndex(pieceIndex);

        Segment segment = segments[bitSetIndex(pieceIndex)];
        int position = bitSetPosition(pieceIndex);
        synchronized (segment) {
            if (segment.skipped == null) {
                segment.skipped = new BitSet(segment.bitSet.length());
            }
            if (!segment.skipped.get(position)) {
                segment.skipped.set(position);
                // sequence between dec pieceCompleted and inc pieceSkipped is critical
                if (segment.bitSet.get(position)) {
                    pieceCompleted.decrementAndGet();
                }
                pieceSkipped.incrementAndGet();
            }
        }
    }

    /**
     * Mark a piece as not skipped
     *
     * @since 1.7
     */
    public void unskip(int pieceIndex) {
        validatePieceIndex(pieceIndex);

        Segment segment = segments[bitSetIndex(pieceIndex)];
        if (segment.skipped == null) {
            return;
        }
        int position = bitSetPosition(pieceIndex);
        synchronized (segment) {
            if (segment.skipped.get(position)) {
                segment.skipped.set(position, false);
                // sequence between dec pieceSkipped and inc pieceCompleted is critical
                pieceSkipped.decrementAndGet();
                if (segment.bitSet.get(position)) {
                    pieceCompleted.incrementAndGet();
                }
            }
        }
    }

    public boolean isSkipped(int pieceIndex) {
        validatePieceIndex(pieceIndex);

        Segment segment = segments[bitSetIndex(pieceIndex)];
        if(segment.skipped == null){
            return false;
        }

        int position = bitSetPosition(pieceIndex);
        synchronized (segment) {
            return segment.skipped.get(position);
        }
    }

    private static int bitSetIndex(int pieceIndex) {
        return pieceIndex & MASK;
    }

    private static int bitSetPosition(int pieceIndex) {
        return pieceIndex >> BIT_COUNT;
    }

    private static class Segment {
        public Segment(int size) {
            this.bitSet = new BitSet(size);
        }

        BitSet bitSet;
        volatile BitSet skipped = null;
    }
}
