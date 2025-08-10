using System;
using System.Collections.Specialized;
using System.Runtime.Caching;

namespace MSSQL.QueryBuilder
{
    /*** SqlQueryCache class
    * 
    * This class is used to cache clauses
    * Author: Phan Xuân Chánh { Chinese Charater: 潘春正, EnglishName1: Chanh Xuan Phan, EnglishName2: StevePhan }
    *  - www.phanxuanchanh.com
    *  
    */
    internal class SqlQueryCache
    {
        private static readonly MemoryCache _cache = new MemoryCache("SqlQueryCache", new NameValueCollection
        {
            { "cacheMemoryLimitMegabytes", "0" },
            { "physicalMemoryLimitPercentage", "0" },
            { "pollingInterval", "00:02:00" },
            { "cacheEntriesCountLimit", "2000" }
        });


        public static void AddClause<TKey, TValue>(TKey key, TValue value)
        {
            CacheItem cacheItem = new CacheItem(key.ToString(), value);
            CacheItemPolicy policy = new CacheItemPolicy
            {
                AbsoluteExpiration = ObjectCache.InfiniteAbsoluteExpiration,
                Priority = CacheItemPriority.Default
            };

            _cache.Add(cacheItem, policy);
        }

        public static bool TryGetClause<TKey, TValue>(TKey key, out TValue value)
        {
            CacheItem cacheItem = _cache.GetCacheItem(key.ToString());
            if (cacheItem == null)
            {
                value = default(TValue);
                return false;
            }

            value = (TValue)cacheItem.Value;
            return true;
        }

        internal enum ClauseTypes { Select, SelectCount, Insert, Update, Delete, OrderBy, Where }

        internal class ClauseCacheKey
        {
            public Type Type { get; set; }
            public string Expression { get; set; }
            public ClauseTypes ClauseTypes { get; set; }

            public override string ToString()
            {
                return $"[{ClauseTypes.ToString()}]{{ Type:{Type.ToString()};Selector:{Expression}}}";
            }
        }

        internal class ClauseCacheValue
        {
            public string Clause { get; set; }
        }
    }
}
