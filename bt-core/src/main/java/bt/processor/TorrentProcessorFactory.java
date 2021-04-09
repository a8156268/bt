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

package bt.processor;

import bt.event.EventSink;
import bt.event.EventSource;
import bt.metainfo.IMetadataService;
import bt.module.ClientExecutor;
import bt.module.MessagingAgents;
import bt.net.IConnectionSource;
import bt.net.IMessageDispatcher;
import bt.net.IPeerConnectionPool;
import bt.net.pipeline.IBufferedPieceRegistry;
import bt.peer.IPeerRegistry;
import bt.processor.magnet.FetchMetadataStage;
import bt.processor.magnet.InitializeMagnetTorrentProcessingStage;
import bt.processor.magnet.MagnetContext;
import bt.processor.magnet.ProcessMagnetTorrentStage;
import bt.processor.torrent.ChooseFilesStage;
import bt.processor.torrent.CreateSessionStage;
import bt.processor.torrent.FetchTorrentStage;
import bt.processor.torrent.InitializeTorrentProcessingStage;
import bt.processor.torrent.ProcessTorrentStage;
import bt.processor.torrent.SeedStage;
import bt.processor.torrent.TorrentContext;
import bt.processor.torrent.TorrentContextFinalizer;
import bt.runtime.Config;
import bt.torrent.TorrentRegistry;
import bt.torrent.data.DataWorker;
import bt.torrent.messaging.IAssignmentFactory;
import bt.tracker.ITrackerService;
import com.google.inject.Inject;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

public class TorrentProcessorFactory implements ProcessorFactory {

    private TorrentRegistry torrentRegistry;
    private DataWorker dataWorker;
    private IBufferedPieceRegistry bufferedPieceRegistry;
    private ITrackerService trackerService;
    private ExecutorService executor;
    private IPeerRegistry peerRegistry;
    private IConnectionSource connectionSource;
    private IPeerConnectionPool connectionPool;
    private IMessageDispatcher messageDispatcher;
    private Set<Object> messagingAgents;
    private IMetadataService metadataService;
    private IAssignmentFactory assignmentFactory;
    private EventSource eventSource;
    private EventSink eventSink;
    private Config config;

    private final Map<Class<?>, Processor<?>> processors;

    @Inject
    public TorrentProcessorFactory(TorrentRegistry torrentRegistry,
                                   DataWorker dataWorker,
                                   IBufferedPieceRegistry bufferedPieceRegistry,
                                   ITrackerService trackerService,
                                   @ClientExecutor ExecutorService executor,
                                   IPeerRegistry peerRegistry,
                                   IConnectionSource connectionSource,
                                   IPeerConnectionPool connectionPool,
                                   IMessageDispatcher messageDispatcher,
                                   @MessagingAgents Set<Object> messagingAgents,
                                   IMetadataService metadataService,
                                   IAssignmentFactory assignmentFactory,
                                   EventSource eventSource,
                                   EventSink eventSink,
                                   Config config) {
        this.torrentRegistry = torrentRegistry;
        this.dataWorker = dataWorker;
        this.bufferedPieceRegistry = bufferedPieceRegistry;
        this.trackerService = trackerService;
        this.executor = executor;
        this.peerRegistry = peerRegistry;
        this.connectionSource = connectionSource;
        this.connectionPool = connectionPool;
        this.messageDispatcher = messageDispatcher;
        this.messagingAgents = messagingAgents;
        this.metadataService = metadataService;
        this.assignmentFactory = assignmentFactory;
        this.eventSource = eventSource;
        this.eventSink = eventSink;
        this.config = config;

        this.processors = processors();
    }

    private Map<Class<?>, Processor<?>> processors() {
        Map<Class<?>, Processor<?>> processors = new HashMap<>();

        processors.put(TorrentContext.class, createTorrentProcessor());
        processors.put(MagnetContext.class, createMagnetProcessor());

        return processors;
    }

    protected ChainProcessor<TorrentContext> createTorrentProcessor() {

        ProcessingStage<TorrentContext> stage5 = new SeedStage<>(null, torrentRegistry);

        ProcessingStage<TorrentContext> stage4 = new ProcessTorrentStage<>(stage5, torrentRegistry, trackerService, eventSink);

        ProcessingStage<TorrentContext> stage3 = new ChooseFilesStage<>(stage4, torrentRegistry, assignmentFactory, config);

        ProcessingStage<TorrentContext> stage2 = new InitializeTorrentProcessingStage<>(stage3, connectionPool,
                torrentRegistry, dataWorker, bufferedPieceRegistry, eventSink, config);

        ProcessingStage<TorrentContext> stage1 = new CreateSessionStage<>(stage2, torrentRegistry, eventSource,
                connectionSource, messageDispatcher, messagingAgents, config);

        ProcessingStage<TorrentContext> stage0 = new FetchTorrentStage(stage1, eventSink);

        return new ChainProcessor<>(stage0, executor, new TorrentContextFinalizer<>(torrentRegistry, eventSink));
    }

    protected ChainProcessor<MagnetContext> createMagnetProcessor() {

        ProcessingStage<MagnetContext> stage5 = new SeedStage<>(null, torrentRegistry);

        ProcessingStage<MagnetContext> stage4 = new ProcessMagnetTorrentStage(stage5, torrentRegistry, trackerService, eventSink);

        ProcessingStage<MagnetContext> stage3 = new ChooseFilesStage<>(stage4, torrentRegistry, assignmentFactory, config);

        ProcessingStage<MagnetContext> stage2 = new InitializeMagnetTorrentProcessingStage(stage3, connectionPool,
                torrentRegistry, dataWorker, bufferedPieceRegistry, eventSink, config);

        ProcessingStage<MagnetContext> stage1 = new FetchMetadataStage(stage2, metadataService, torrentRegistry,
                peerRegistry, eventSink, eventSource, config);

        ProcessingStage<MagnetContext> stage0 = new CreateSessionStage<>(stage1, torrentRegistry, eventSource,
                connectionSource, messageDispatcher, messagingAgents, config);

        return new ChainProcessor<>(stage0, executor, new TorrentContextFinalizer<>(torrentRegistry, eventSink));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <C extends ProcessingContext> Processor<C> processor(Class<C> contextType) {
        return (Processor<C>) processors.get(contextType);
    }
}
