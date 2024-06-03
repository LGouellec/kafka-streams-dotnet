// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// This is a fork from Microsoft.Extensions.Caching.Memory.MemoryCache https://github.com/dotnet/runtime/blob/main/src/libraries/Microsoft.Extensions.Caching.Memory
// The only difference is the compaction process and eviction callback is synchronous whereas the .NET repo is asyncrhonous

using System;
using Microsoft.Extensions.Options;

namespace Streamiz.Kafka.Net.State.Cache.Internal
{
    /// <summary>
    /// Options class for <see cref="MemoryCache"/>.
    /// </summary>
    internal class MemoryCacheOptions : IOptions<MemoryCacheOptions>
    {
        private long _sizeLimit = NotSet;
        private double _compactionPercentage = 0.05;

        private const int NotSet = -1;

        /// <summary>
        /// Gets or sets the clock used by the cache for expiration.
        /// </summary>
        /// Add in .Net 8
        //public ISystemClock? Clock { get; set; }

        /// <summary>
        /// Gets or sets the minimum length of time between successive scans for expired items.
        /// </summary>
        public TimeSpan ExpirationScanFrequency { get; set; } = TimeSpan.FromMinutes(1);

        internal bool HasSizeLimit => _sizeLimit >= 0;

        internal long SizeLimitValue => _sizeLimit;

        /// <summary>
        /// Gets or sets the maximum size of the cache.
        /// </summary>
        public long? SizeLimit
        {
            get => _sizeLimit < 0 ? null : _sizeLimit;
            set
            {
                if (value < 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), value, $"{nameof(value)} must be non-negative.");
                }

                _sizeLimit = value ?? NotSet;
            }
        }
        
        /// <summary>
        /// Gets or sets the amount to compact the cache by when the maximum size is exceeded.
        /// </summary>
        public double CompactionPercentage
        {
            get => _compactionPercentage;
            set
            {
                if (value is < 0 or > 1)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), value, $"{nameof(value)} must be between 0 and 1 inclusive.");
                }

                _compactionPercentage = value;
            }
        }
        
        /// <summary>
        /// Gets or sets whether to track memory cache statistics. Disabled by default.
        /// </summary>
        public bool TrackStatistics { get; set; }

        MemoryCacheOptions IOptions<MemoryCacheOptions>.Value => this;
    }
}
