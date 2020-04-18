using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using Dasync.Collections;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Polly;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Web;
using System.Web.Script.Serialization;

namespace Parallelism
{
    class Program
    {
        static void Main()
        {
            BenchmarkRunner.Run<Parallelism>();            

            System.Console.ReadKey();
        }      
    }

     [SimpleJob(RuntimeMoniker.Net461, baseline: true)]
    public class Parallelism
    {
        public IEnumerable<string> Data = "1;2;3;4;5;6;7;8;9;10".Split(';').ToArray();
        public int MaxThreads = 4;

        [Benchmark]
        public void ParallelForEach() => ParallelForEachTask(Data, MaxThreads);


        [Benchmark]
        public Task ConcurrentQueue() => ConcurrentQueueTask(Data, MaxThreads);

        [Benchmark]
        public Task ConcurrentPartitioner() => ConcurrentPartitionerTask(Data, MaxThreads);

        [Benchmark]
        public Task ActionBlock() => ActionBlockTask(Data, MaxThreads);

        [Benchmark]
        public Task Semaphore() => SemaphoreTask(Data, MaxThreads);

        [Benchmark]
        public Task ParallelForEachAsync() => ParallelForEachAsyncTask(Data, MaxThreads);

        [Benchmark]
        public Task PollyBulkhead() => PollyBulkheadTask(Data, MaxThreads);


        // Work only in Sync methods
        public void ParallelForEachTask(IEnumerable<string> source, int maxThreads)
        {
            Parallel.ForEach(Data, new ParallelOptions() { MaxDegreeOfParallelism = MaxThreads }, x => WriteAsync(x).Wait());
        }
        public async Task ConcurrentQueueTask(IEnumerable<string> source, int maxThreads)
        {
            var q = new ConcurrentQueue<string>(source);
            var tasks = new List<Task>();

            for (int n = 0; n < maxThreads; n++)
            {
                tasks.Add(Task.Run(async () =>
                {

                    while (q.TryDequeue(out string x))
                    {
                        await WriteAsync(x);
                    }
                }));
            }

            await Task.WhenAll(tasks);
        }

        public async Task ConcurrentPartitionerTask(IEnumerable<string> source, int maxThreads)
        {
            var tasks = Partitioner.Create(source)
                .GetPartitions(maxThreads)
                .Select(partition => Task.Run(async () =>
                {
                    using (partition)
                    {
                        while (partition.MoveNext())
                        {
                            await WriteAsync(partition.Current);
                        }
                    }
                }
                ));

            await Task.WhenAll(tasks);
        }

        public async Task ActionBlockTask(IEnumerable<string> source, int maxThreads)
        {
            var actionBlock = new ActionBlock<string>(WriteAsync, new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = maxThreads });

            foreach (var x in source)
            {
                await actionBlock.SendAsync(x);
            }

            actionBlock.Complete();
            await actionBlock.Completion;
        }

        public async Task SemaphoreTask(IEnumerable<string> source, int maxThreads)
        {
            var allTasks = new List<Task>();
            var throttler = new SemaphoreSlim(initialCount: maxThreads);
            foreach (var x in source)
            {
                await throttler.WaitAsync();
                allTasks.Add(
                    Task.Run(async () =>
                    {
                        try
                        {
                            await WriteAsync(x);
                        }
                        finally
                        {
                            throttler.Release();
                        }
                    }));
            }
            await Task.WhenAll(allTasks);
        }

        // Using AsyncEnumerator Library
        public async Task ParallelForEachAsyncTask(IEnumerable<string> source, int maxThreads)
        {
            await source.ParallelForEachAsync(async x => await WriteAsync(x), maxThreads);
        }

        // Using Polly Library
        public async Task PollyBulkheadTask(IEnumerable<string> source, int maxThreads)
        {
            var bulkhead = Policy.BulkheadAsync(maxThreads, Int32.MaxValue);
            var tasks = new List<Task>();
            foreach (var x in source)
            {
                var t = bulkhead.ExecuteAsync(async () =>
                {
                    await WriteAsync(x);
                });
                tasks.Add(t);
            }
            await Task.WhenAll(tasks);
        }

        public async Task WriteAsync(string text)
        {
            await Task.Run(async () =>
            {
                await Task.Delay(1000);

                System.Console.WriteLine(text);
            });
        }
    }
}
