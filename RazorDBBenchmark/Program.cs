using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using RazorDB;
using System;
using System.IO;
using System.Text;

namespace RazorDBBenchmark
{
    public class Program
    {
        static void Main(string[] args)
        {
            var summary = BenchmarkRunner.Run<MemoryEfficiencyTests>();
        }

        public static string TestFolder => Path.Combine(Directory.GetCurrentDirectory(), "BenchmarkData");
    }

    [MemoryDiagnoser]
    public class MemoryEfficiencyTests : IDisposable
    {
        private string _testFolder;
        private KeyValueStore _db;
        private byte[] _value;

        public MemoryEfficiencyTests()
        {
            _testFolder = Path.Combine(Program.TestFolder, "memdb");
            _db = new KeyValueStore(_testFolder);
            _value = Encoding.UTF8.GetBytes("Test RazorDB Value");
        }

        public void Clean()
        {
            if (Directory.Exists(_testFolder))
                Directory.Delete(_testFolder, true);
        }

        public void Dispose() => _db.Dispose();

        [Benchmark]
        public void OneHundredKSmallKeyWriteRead()
        {
            for (int i = 0; i < 100_000; i++)
            {
                _db.Set(BitConverter.GetBytes(i), _value);
            }

            for (int i = 0; i < 100_000; i++)
            {
                byte[] val = _db.Get(BitConverter.GetBytes(i));
                if (val.Length != _value.Length)
                    throw new Exception("Invalid test result. Expected value lengths to match");
            }
        }

        /* Sample Run
BenchmarkDotNet=v0.11.5, OS=Windows 10.0.18362
AMD Ryzen 5 1600, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=3.0.100-preview8-013656
  [Host]     : .NET Core 2.2.6 (CoreCLR 4.6.27817.03, CoreFX 4.6.27818.02), 64bit RyuJIT
  DefaultJob : .NET Core 2.2.6 (CoreCLR 4.6.27817.03, CoreFX 4.6.27818.02), 64bit RyuJIT


|                       Method |    Mean |    Error |   StdDev |        Gen 0 |     Gen 1 | Gen 2 | Allocated |
|----------------------------- |--------:|---------:|---------:|-------------:|----------:|------:|----------:|
| OneHundredKSmallKeyWriteRead | 3.870 s | 0.2742 s | 0.8086 s | 2677000.0000 | 7000.0000 |     - |   2.41 GB |
*/
    }
}
