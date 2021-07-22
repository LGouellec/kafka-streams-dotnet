using Confluent.Kafka;
using Moq;
using NUnit.Framework;
using RocksDbSharp;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State.RocksDb;
using Streamiz.Kafka.Net.Tests.Helpers;
using System;
using System.Collections.Generic;
using System.IO;


namespace Streamiz.Kafka.Net.Tests.Public
{
    public class RocksDbOptionsTests
    {
        #region Cst
        private const int parallelism = 1;
        private const uint bloomLocality = 50;
        private const int baseBackgroundCompactions = 100;
        private const int accessHintOnCompactionStart = 1;
        private const ulong blockCacheSizeMb = 100;
        private const ulong memTableMemoryBudget = 5;
        private const ulong arenaBlockSize = 100;
        private const ulong memtableMemoryBudget2 = 6;
        private const ulong bytesPerSync = 600;
        private const bool adviseRandomOnOpen = false;
        private const bool allowConcurrentMemtableWrite = false;
        #endregion

        private StreamConfig config = null;
        private RocksDbKeyValueStore store = null;
        private ProcessorContext context = null;
        private TaskId id = null;
        private TopicPartition partition = null;
        private ProcessorStateManager stateManager = null;
        private Mock<AbstractTask> task = null;

        [SetUp]
        public void Begin()
        {
            Random rd = new Random();
            config = new StreamConfig();
            config.ApplicationId = $"RocksDbOptionsTests";
            config.UseRandomRocksDbConfigForTest();
            config.RocksDbConfigHandler = (name, options) =>
            {
                options
                    .EnableStatistics()
                    .IncreaseParallelism(parallelism)
                    .OptimizeForPointLookup(blockCacheSizeMb)
                    .OptimizeLevelStyleCompaction(memTableMemoryBudget)
                    .OptimizeUniversalStyleCompaction(memtableMemoryBudget2)
                    .PrepareForBulkLoad()
                    .SetAccessHintOnCompactionStart(accessHintOnCompactionStart)
                    .SetAdviseRandomOnOpen(adviseRandomOnOpen)
                    .SetAllowConcurrentMemtableWrite(allowConcurrentMemtableWrite)
                    .SetAllowMmapReads(false)
                    .SetAllowMmapWrites(false)
                    .SetArenaBlockSize(arenaBlockSize)
                    .SetBaseBackgroundCompactions(baseBackgroundCompactions)
                    .SetBloomLocality(bloomLocality)
                    .SetBytesPerSync(bytesPerSync)
                    // .SetCompactionFilter(IntPtr.Zero)
                    // .SetCompactionFilterFactory(IntPtr.Zero)
                    .SetCompactionReadaheadSize(1200)
                    .SetCompactionStyle(RocksDbSharp.Compaction.Level)
                    //.SetComparator(IntPtr.Zero)
                    .SetCompression(RocksDbSharp.Compression.Lz4)
                    .SetCompressionOptions(1, 2, 3, 4)
                    .SetCompressionPerLevel(new[] { RocksDbSharp.Compression.Lz4 }, 1)
                    .SetCreateIfMissing()
                    .SetCreateMissingColumnFamilies()
                    .SetDbLogDir("test")
                    .SetDbWriteBufferSize(1000)
                    .SetDeleteObsoleteFilesPeriodMicros(50)
                    .SetDisableAutoCompactions(1)
                    .SetEnableWriteThreadAdaptiveYield(true)
                    //.SetEnv(IntPtr.Zero)
                    .SetErrorIfExists()
                    //.SetFifoCompactionOptions(IntPtr.Zero)
                    .SetHardPendingCompactionBytesLimit(1)
                    .SetHardRateLimit(40597)
                    .SetHashLinkListRep(12)
                    .SetHashSkipListRep(56, 4, 2)
                    //.SetInfoLog(IntPtr.Zero)
                    .SetInfoLogLevel(RocksLogLevel.NUM_INFO_LOG)
                    .SetInplaceUpdateNumLocks(134)
                    .SetIsFdCloseOnExec(false)
                    .SetKeepLogFileNum(1)
                    .SetLevel0FileNumCompactionTrigger(14)
                    .SetLevel0SlowdownWritesTrigger(144)
                    .SetInplaceUpdateSupport(true)
                    .SetLevel0StopWritesTrigger(24)
                    .SetLevelCompactionDynamicLevelBytes(true)
                    .SetLogFileTimeToRoll(154)
                    .SetManifestPreallocationSize(153)
                    .SetMaxBackgroundFlushes(3)
                    .SetMaxBytesForLevelBase(1453)
                    .SetMaxBytesForLevelMultiplier(2)
                    .SetMaxBytesForLevelMultiplierAdditional(new[] { 1 }, 1)
                    .SetMaxCompactionBytes(345678)
                    .SetMaxFileOpeningThreads(2)
                    .SetMaxLogFileSize(1)
                    .SetMaxManifestFileSize(131)
                    .SetMaxMemCompactionLevel(12)
                    .SetMaxOpenFiles(20)
                    .SetMaxSequentialSkipInIterations(13)
                    .SetMaxSuccessiveMerges(12)
                    .SetMaxTotalWalSize(12)
                    .SetMaxWriteBufferNumber(15543)
                    .SetMaxWriteBufferNumberToMaintain(126)
                    .SetMemtableHugePageSize(64317)
                    .SetMemtableHugePageSize(64317)
                    .SetMemtablePrefixBloomSizeRatio(12)
                    //.SetMergeOperator(IntPtr.Zero)
                    .SetMinLevelToCompress(1)
                    .SetMemtableVectorRep()
                    .SetMinWriteBufferNumberToMerge(1312)
                    .SetNumLevels(1)
                    .SetOptimizeFiltersForHits(56)
                    .SetParanoidChecks()
                    .SetSkipLogErrorOnRecovery(true)
                    .SetSoftPendingCompactionBytesLimit(134)
                    .SetSoftRateLimit(131)
                    .SetStatsDumpPeriodSec(532)
                    .SetTableCacheNumShardbits(12)
                    .SetTableCacheRemoveScanCountLimit(66)
                    .SetTargetFileSizeBase(6)
                    .SetTargetFileSizeMultiplier(2)
                    .SetUint64addMergeOperator()
                    //.SetUniversalCompactionOptions(IntPtr.Zero)
                    .SetUseAdaptiveMutex(false)
                    .SetUseDirectIoForFlushAndCompaction(true)
                    .SetUseDirectReads(true)
                    .SetUseFsync(1)
                    .SetWalRecoveryMode(RocksDbSharp.Recovery.SkipAnyCorruptedRecords)
                    .SetWALSizeLimitMB(40)
                    .SetWALTtlSeconds(1454151413)
                    .SetWriteBufferSize(45678976543)
                    .SetPrefixExtractor(SliceTransform.CreateNoOp())
                    .SetPurgeRedundantKvsWhileFlush(false)
                    .SetRateLimitDelayMaxMilliseconds(1762)
                    .SetRecycleLogFileNum(1)
                    .SetReportBgIoStats(true)
                    .SetPlainTableFactory(1, 23, 4, 2)
                    .SetMaxBackgroundCompactions(1);
            };

            id = new TaskId { Id = 0, Partition = 0 };
            partition = new TopicPartition("source", 0);
            stateManager = new ProcessorStateManager(id, new List<TopicPartition> { partition }, null);

            task = new Mock<AbstractTask>();
            task.Setup(k => k.Id).Returns(id);

            context = new ProcessorContext(task.Object, config, stateManager);

            store = new RocksDbKeyValueStore("test-store");
            store.Init(context, store);
        }

        [TearDown]
        public void End()
        {
            store.Flush();
            stateManager.Close();
            Directory.Delete(Path.Combine(config.StateDir, config.ApplicationId), true);
        }

        [Test]
        public void RocksDbOptionsTest()
        {
            int a = 1;
        }

    }
}
