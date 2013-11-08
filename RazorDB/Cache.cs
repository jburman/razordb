/*
Copyright 2012, 2013 Gnoso Inc.

This software is licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except for what is in compliance with the License.

You may obtain a copy of this license at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either expressed or implied.

See the License for the specific language governing permissions and limitations.
*/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RazorDB {

    internal class CacheEntry<T> {
        internal string Key;
        internal T Value;
        internal int Size;
        internal LinkedListNode<CacheEntry<T>> ListNode;
    }

    public class Cache<T> {
        public Cache(int sizeLimit, Func<T,int> sizer) {
            _sizeLimit = sizeLimit;
            _sizer = sizer;
        }
        private int _sizeLimit;
        private int _currentSize;
        public int CurrentSize {
            get { return _currentSize; }
        }
        private Func<T, int> _sizer;

        private Dictionary<string, CacheEntry<T>> _hash = new Dictionary<string, CacheEntry<T>>();
        private LinkedList<CacheEntry<T>> _list = new LinkedList<CacheEntry<T>>();
        private object _lock = new object();

        public bool TryGetValue(string key, out T value) {
            lock (_lock) {
                CacheEntry<T> val;
                bool exists = _hash.TryGetValue(key, out val);
                if (exists) {
                    // move the item to the top of the LRU list
                    _list.Remove(val.ListNode);
                    _list.AddFirst(val.ListNode);
                }
                // Set the output parameter
                value = exists ? val.Value : default(T);
                return exists;
            }
        }

        public void Set(string key, T value) {
            lock (_lock) {

                // If the hash already contains the key, we are probably in a race condition, so go ahead and abort.
                if (_hash.ContainsKey(key))
                    return;

                var cacheEntry = new CacheEntry<T> { Value = value, Size = _sizer(value), Key = key };
                var node = _list.AddFirst(cacheEntry);
                cacheEntry.ListNode = node;
                _hash.Add(key, cacheEntry);
                _currentSize += _sizer(value);

                CheckCacheSizeAndEvict();
            }
        }

        /// <summary>
        /// Removes all cache entries where the supplied function returns true for the cache key.
        /// </summary>
        /// <param name="predicate">A function to test the cache key for whether it should be removed or not.</param>
        public void RemoveWhere(Func<string, bool> predicate) {
            lock (_lock) {
                var entriesToRemove = new Stack<CacheEntry<T>>();
                foreach (var cacheEntry in _list) {
                    if (predicate(cacheEntry.Key)) {
                        entriesToRemove.Push(cacheEntry);
                    }
                }

                foreach (var remove in entriesToRemove) {
                    _list.Remove(remove);
                    _hash.Remove(remove.Key);
                    _currentSize -= remove.Size;
                }
            }
        }

        private void CheckCacheSizeAndEvict() {
            while (_currentSize > _sizeLimit) {

                var lastEntry = _list.Last;
                var cacheEntry = lastEntry.Value;

                // Subtract the last entry from the size
                _currentSize -= cacheEntry.Size;
                // Remove from list
                _list.RemoveLast();
                // Remove from hash
                _hash.Remove(cacheEntry.Key);
            }
        }
    }
    
    public class RazorCache {

        public RazorCache() : this(Config.IndexCacheSize, Config.DataBlockCacheSize) {
        }

        public RazorCache(int indexCacheSize, int dataBlockCacheSize) {
            if (indexCacheSize < 0) { throw new ArgumentOutOfRangeException("indexCacheSize"); }
            if (dataBlockCacheSize < 0) { throw new ArgumentOutOfRangeException("dataBlockCacheSize"); }

            _blockIndexCache = new Cache<Key[]>(indexCacheSize, index => index.Sum(ba => ba.Length));
            _blockDataCache = new Cache<byte[]>(dataBlockCacheSize, block => block.Length);
        }

        private Cache<Key[]> _blockIndexCache;
        private Cache<byte[]> _blockDataCache;

        public int IndexCacheSize { get { return _blockIndexCache.CurrentSize; } }
        public int DataCacheSize { get { return _blockDataCache.CurrentSize; } }

        public Key[] GetBlockTableIndex(string baseName, int level, int version) {

            string fileName = Config.SortedBlockTableFile(baseName, level, version);
            Key[] index;

            if (_blockIndexCache.TryGetValue(fileName, out index)) {
                return index;
            }

            var sbt = new SortedBlockTable(null, baseName, level, version);
            try {
                index = sbt.GetIndex();
                _blockIndexCache.Set(fileName, index);
                return index;
            } finally {
                sbt.Close();
            }
        }

        public byte[] GetBlock(string baseName, int level, int version, int blockNum) {
            string blockKey = Config.SortedBlockTableFile(baseName, level, version) + ":" + blockNum.ToString();
            byte[] block = null;
            _blockDataCache.TryGetValue(blockKey, out block);
            return block;
        }

        public void SetBlock(string baseName, int level, int version, int blockNum, byte[] block) {
            try {
                string blockKey = Config.SortedBlockTableFile(baseName, level, version) + ":" + blockNum.ToString();
                _blockDataCache.Set(blockKey, block);
            } catch (Exception ex) {
                if (Config.ExceptionHandling == ExceptionHandling.ThrowAll)
                    throw;
                if (Config.Logger != null)
                    Config.Logger(string.Format("RazorCache.SetBlock Failed: {0}\nException: {1}", baseName, ex.Message));
            }
        }

        /// <summary>
        /// Removes all cache entries related to the supplied Key Value Store "base name".
        /// </summary>
        /// <param name="baseName"></param>
        public void Truncate(string baseName) {
            try {
                string sortedBlockTableDir = Config.SortedBlockTableDir(baseName);

                _blockIndexCache.RemoveWhere((cacheKey) => {
                    return cacheKey.StartsWith(sortedBlockTableDir);
                });

                _blockDataCache.RemoveWhere((cacheKey) => {
                    return cacheKey.StartsWith(sortedBlockTableDir);
                });
            } catch (Exception ex) {
                if (Config.ExceptionHandling == ExceptionHandling.ThrowAll)
                    throw;
                if (Config.Logger != null)
                    Config.Logger(string.Format("RazorCache.Truncate Failed: {0}\nException: {1}", baseName, ex.Message));
            }
        }
    }
}
