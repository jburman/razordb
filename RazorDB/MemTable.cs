﻿using System;
using System.Collections.Generic;
using System.Linq;

namespace RazorDB {

    public class MemTable {

        private Dictionary<ByteArray, ByteArray> _internalTable = new Dictionary<ByteArray,ByteArray>();
        private int _totalKeySize = 0;
        private int _totalValueSize = 0;

        public void Add(ByteArray key, ByteArray value) {
            _totalKeySize += key.Length;
            _totalValueSize += value.Length;

            _internalTable.Add( key, value );
        }

        public bool Lookup(ByteArray key, out ByteArray value) {
            return _internalTable.TryGetValue(key, out value);
        }

        public int Size {
            get { return _totalKeySize + _totalValueSize; }
        }

        public bool Full {
            get { return Size > Config.MaxMemTableSize; }
        }

        public void WriteToSortedBlockTable(string fileName) {

            SortedBlockTableWriter tableWriter = null;
            try {
                tableWriter = new SortedBlockTableWriter(fileName);

                foreach (var pair in _internalTable.OrderBy((pair) => pair.Key)) {
                    tableWriter.WritePair(pair.Key, pair.Value);
                }
            } finally {
                tableWriter.Close();
            }
        }
    }
}