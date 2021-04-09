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

package bt.peerexchange;

import bt.BtException;
import bt.event.EventSource;
import bt.metainfo.TorrentId;
import bt.net.ConnectionKey;
import bt.net.Peer;
import bt.peer.PeerSource;
import bt.peer.PeerSourceFactory;
import bt.protocol.Message;
import bt.protocol.extended.ExtendedHandshake;
import bt.runtime.Config;
import bt.service.IRuntimeLifecycleBinder;
import bt.torrent.annotation.Consumes;
import bt.torrent.annotation.Produces;
import bt.torrent.messaging.MessageContext;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * <p><b>Note that this class implements a service.
 * Hence, is not a part of the public API and is a subject to change.</b></p>
 */
public class PeerExchangePeerSourceFactory implements PeerSourceFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(PeerExchangePeerSourceFactory.class);

    private static final Duration CLEANER_INTERVAL = Duration.ofSeconds(37);

    private Map<TorrentId, PeerExchangePeerSource> peerSources;

    private Map<TorrentId, Queue<PeerEvent>> peerEvents;

    private Map<TorrentId, Set<PeerWrapper>> connectedPeers;

    private ReentrantReadWriteLock rwLock;

    private Set<ConnectionKey> peers;
    private Map<ConnectionKey, Long> lastSentPEXMessage;

    private Duration minMessageInterval;
    private Duration maxMessageInterval;
    private int minEventsPerMessage;
    private int maxEventsPerMessage;

    @Inject
    public PeerExchangePeerSourceFactory(EventSource eventSource,
                                         IRuntimeLifecycleBinder lifecycleBinder,
                                         PeerExchangeConfig pexConfig,
                                         Config config) {
        this.peerSources = new ConcurrentHashMap<>();
        this.peerEvents = new ConcurrentHashMap<>();
        this.connectedPeers = new ConcurrentHashMap<>();
        this.rwLock = new ReentrantReadWriteLock();
        this.peers = ConcurrentHashMap.newKeySet();
        this.lastSentPEXMessage = new ConcurrentHashMap<>();
        if (pexConfig.getMaxMessageInterval().compareTo(pexConfig.getMinMessageInterval()) < 0) {
            throw new IllegalArgumentException("Max message interval is greater than min interval");
        }
        this.minMessageInterval = pexConfig.getMinMessageInterval();
        this.maxMessageInterval = pexConfig.getMaxMessageInterval();
        this.minEventsPerMessage = pexConfig.getMinEventsPerMessage();
        this.maxEventsPerMessage = pexConfig.getMaxEventsPerMessage();

        eventSource.onPeerConnected(null, e -> onPeerConnected(e.getConnectionKey()))
                .onPeerDisconnected(null, e -> onPeerDisconnected(e.getConnectionKey()))
                .onTorrentStopped(null, e -> onTorrentStopped(e.getTorrentId()));

        initCleaner(lifecycleBinder, config);
    }

    private void initCleaner(IRuntimeLifecycleBinder lifecycleBinder, Config config) {
        String threadName = String.format("%d.bt.peerexchange.cleaner", config.getAcceptorPort());
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, threadName));
        lifecycleBinder.onStartup("Schedule periodic cleanup of PEX messages", () -> executor.scheduleAtFixedRate(
                new Cleaner(), CLEANER_INTERVAL.toMillis(), CLEANER_INTERVAL.toMillis(), TimeUnit.MILLISECONDS));
        lifecycleBinder.onShutdown("Shutdown PEX cleanup scheduler", executor::shutdownNow);
    }

    private void onPeerConnected(ConnectionKey connectionKey) {
        getPeerEvents(connectionKey.getTorrentId())
                .add(PeerEvent.added(connectionKey.getPeer()));
    }

    private void onPeerDisconnected(ConnectionKey connectionKey) {
        getPeerEvents(connectionKey.getTorrentId())
                .add(PeerEvent.dropped(connectionKey.getPeer()));
        peers.remove(connectionKey);
        lastSentPEXMessage.remove(connectionKey);
    }

    private void onTorrentStopped(TorrentId torrentId) {
        peerSources.remove(torrentId);
        peerEvents.remove(torrentId);
        connectedPeers.remove(torrentId);
    }

    private Queue<PeerEvent> getPeerEvents(TorrentId torrentId) {
        return peerEvents.computeIfAbsent(torrentId, key -> new PriorityBlockingQueue<>());
    }

    @Override
    public PeerSource getPeerSource(TorrentId torrentId) {
        return getOrCreatePeerSource(torrentId);
    }

    private PeerExchangePeerSource getOrCreatePeerSource(TorrentId torrentId) {
        PeerExchangePeerSource peerSource = peerSources.get(torrentId);
        if (peerSource == null) {
            peerSource = new PeerExchangePeerSource();
            PeerExchangePeerSource existing = peerSources.putIfAbsent(torrentId, peerSource);
            if (existing != null) {
                peerSource = existing;
            }
        }
        return peerSource;
    }

    @Consumes
    public void consume(ExtendedHandshake handshake, MessageContext messageContext) {
        if (handshake.getSupportedMessageTypes().contains("ut_pex")) {
            // TODO: peer may eventually turn off the PEX extension
            // moreover the extended handshake message type map is additive,
            // so we can't learn about the peer turning off extensions solely from the message
            peers.add(messageContext.getConnectionKey());
        }
    }

    @Consumes
    public void consume(PeerExchange message, MessageContext messageContext) {
        getOrCreatePeerSource(messageContext.getTorrentId()).addMessage(message);
    }

    @Produces
    public void produce(Consumer<Message> messageConsumer, MessageContext messageContext) {
        ConnectionKey connectionKey = messageContext.getConnectionKey();
        Peer remotePeer = connectionKey.getPeer();
        if (remotePeer.isPortUnknown()) {
            return;
        }

        long currentTime = System.currentTimeMillis();
        long lastSentPEXMessageToPeer = lastSentPEXMessage.getOrDefault(connectionKey, 0L);

        if (peers.contains(connectionKey)
                && ((currentTime - lastSentPEXMessageToPeer) >= minMessageInterval.toMillis() || lastSentPEXMessageToPeer == 0L)) {
            rwLock.readLock().lock();

            if (lastSentPEXMessageToPeer == 0L) {
                sendConnectedPeers(messageConsumer, messageContext, remotePeer);
                lastSentPEXMessage.put(connectionKey, 1L);
            }

            List<PeerEvent> events = new ArrayList<>();
            try {
                Queue<PeerEvent> torrentPeerEvents = getPeerEvents(messageContext.getTorrentId());
                for (PeerEvent event : torrentPeerEvents) {
                    if (event.getInstant() - lastSentPEXMessageToPeer >= 0) {
                        Peer exchangedPeer = event.getPeer();
                        if (shouldSendPexMessage(exchangedPeer, remotePeer)) {
                            events.add(event);
                        }
                    } else {
                        break;
                    }
                    if (events.size() >= maxEventsPerMessage) {
                        break;
                    }
                }
            } finally {
                rwLock.readLock().unlock();
            }

            if (events.size() >= minEventsPerMessage ||
                    (!events.isEmpty() && (currentTime - lastSentPEXMessageToPeer >= maxMessageInterval.toMillis()))) {
                lastSentPEXMessage.put(connectionKey, currentTime);
                PeerExchange.Builder messageBuilder = PeerExchange.builder();
                events.forEach(event -> {
                    switch (event.getType()) {
                        case ADDED: {
                            messageBuilder.added(event.getPeer());
                            break;
                        }
                        case DROPPED: {
                            messageBuilder.dropped(event.getPeer());
                            break;
                        }
                        default: {
                            throw new BtException("Unknown event type: " + event.getType().name());
                        }
                    }
                });
                messageConsumer.accept(messageBuilder.build());
            }
        }
    }

    private void sendConnectedPeers(Consumer<Message> messageConsumer, MessageContext messageContext, Peer remotePeer) {
        Set<PeerWrapper> peers = connectedPeers.get(messageContext.getTorrentId());
        if (peers == null) {
            return;
        }
        PeerExchange.Builder messageBuilder = PeerExchange.builder();
        int eventsCount = 0;
        for (PeerWrapper exchangedPeer : peers) {
            if (shouldSendPexMessage(exchangedPeer.getPeer(), remotePeer)) {
                messageBuilder.added(exchangedPeer.getPeer());
                ++eventsCount;
                if (eventsCount >= maxEventsPerMessage) {
                    PeerExchange exchange = messageBuilder.build();
                    messageConsumer.accept(exchange);
                    messageBuilder = PeerExchange.builder();
                    eventsCount = 0;
                }
            }
        }
        if (eventsCount > 0) {
            PeerExchange exchange = messageBuilder.build();
            messageConsumer.accept(exchange);
        }
    }

    private class Cleaner implements Runnable {

        @Override
        public void run() {
            rwLock.writeLock().lock();
            try {
                long lruEventTime = lastSentPEXMessage.values().stream()
                        .reduce(Long.MAX_VALUE, (a, b) -> (a < b) ? a : b);

                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Prior to cleaning events. LRU event time: {}, peer events: {}", lruEventTime, peerEvents);
                }

                PeerEvent event;
                for (Map.Entry<TorrentId, Queue<PeerEvent>> entry : peerEvents.entrySet()) {
                    TorrentId torrentId = entry.getKey();
                    Queue<PeerEvent> events = entry.getValue();
                    while ((event = events.peek()) != null && event.getInstant() <= lruEventTime) {
                        Set<PeerWrapper> peers = connectedPeers.computeIfAbsent(torrentId, key -> ConcurrentHashMap.newKeySet());
                        if (event.getType() == PeerEvent.Type.ADDED) {
                            peers.add(new PeerWrapper(event.getPeer()));
                        } else if (event.getType() == PeerEvent.Type.DROPPED) {
                            peers.remove(new PeerWrapper(event.getPeer()));
                        }
                        events.poll();
                    }
                }

                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("After cleaning events. Peer events: {}", peerEvents);
                }

            } finally {
                rwLock.writeLock().unlock();
            }
        }

    }

    // don't send PEX message if anything of the above is true:
    // - we don't know the listening port of the event's peer yet
    // - we don't know the listening port of the current connection's peer yet
    // - event's peer and connection's peer are the same
    private boolean shouldSendPexMessage(Peer peer, Peer remotePeer) {
        if (peer.isPortUnknown()) {
            return false;
        }

        return !PeerWrapper.equals(peer, remotePeer);
    }
}
