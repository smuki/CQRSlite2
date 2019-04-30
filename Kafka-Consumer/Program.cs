using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka.Serialization;


/// <summary>
///     演示如何使用Consumer客户端.
/// </summary>
namespace Confluent.Kafka.Examples.Consumer
{
    public class Program
    {
        /// <summary>
        //      在这个例子中:
        ///         - offsets 是自动提交的。
        ///         - consumer.Poll / OnMessage 是用于消息消费的。
        ///         - 没有为轮询循环创建（Poll）二外的线程，当然可以创建
        /// </summary>
        public static void Run_Poll(string brokerList, List<string> topics)
        {
            var config = new Dictionary<string, object>
            {
                { "bootstrap.servers", brokerList },
                { "group.id", "csharp-consumer" },
                { "enable.auto.commit", true },  // 默认值
                { "auto.commit.interval.ms", 5000 },
                { "statistics.interval.ms", 60000 },
                { "session.timeout.ms", 6000 },
                { "auto.offset.reset", "smallest" }
            };

            using (var consumer = new Consumer<Ignore, string>(config, null, new StringDeserializer(Encoding.UTF8)))
            {
                // 注意: 所有事件处理程序的执行都是在主线程中执行的，就是同步的。

                //当成功消费了消息就会触发该事件
                consumer.OnMessage += (_, msg) => Console.WriteLine("Topic: "+msg.Topic+" Partition: "+msg.Partition+" Offset: "+msg.Offset+" "+msg.Value);

                consumer.OnPartitionEOF += (_, end) => Console.WriteLine("Reached end of topic "+end.Topic+" partition "+end.Partition+", next message will be at offset "+end.Offset);

                //当然发生了严重错误，比如，连接丢失或者Kafka服务器无效就会触发该事件
                consumer.OnError += (_, error) => Console.WriteLine("Error: "+error);

                //当反序列化有错误，或者消费的过程中发生了错误，即error != NoError，就会触发该事件
                consumer.OnConsumeError += (_, msg)
                    => Console.WriteLine("Error consuming from topic/partition/offset "+msg.Topic+"/"+msg.Partition+"/"+msg.Offset+": "+msg.Error);

                //成功提交了Offsets会触发该事件
                consumer.OnOffsetsCommitted += (_, commit) => Console.WriteLine(commit.Error ? "Failed to commit offsets: "+commit.Error : "Successfully committed offsets: "+commit.Offsets);

                // 当消费者被分配一组新的分区时触发该事件
                consumer.OnPartitionsAssigned += (_, partitions) =>
                {
                    Console.WriteLine("Assigned partitions:"+partitions+"  member id: "+consumer.MemberId);
                    // 如果您未向OnPartitionsAssigned事件添加处理程序，则会自动执行以下.Assign调用。 如果你这样做，你必须明确地调用.Assign以便消费者开始消费消息。
                    //开始从分区中消息消息
                    consumer.Assign(partitions);
                };

                // 当消费者的当前分区集已被撤销时引发该事件。
                consumer.OnPartitionsRevoked += (_, partitions) =>
                {
                    Console.WriteLine("Revoked partitions:"+partitions);
                    // 如果您未向OnPartitionsRevoked事件添加处理程序，则下面的.Unassign调用会自动发生。 如果你这样做了，你必须明确地调用.Usessign以便消费者停止从它先前分配的分区中消费消息。

                    //停止从分区中消费消息
                    consumer.Unassign();
                };

                consumer.OnStatistics += (_, json) => Console.WriteLine("Statistics: "+json);

                consumer.Subscribe(topics);

                Console.WriteLine("Subscribed to:"+consumer.Subscription);

                var cancelled = false;
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true;  // 组织进程退出
                    cancelled = true;
                };

                Console.WriteLine("Ctrl-C to exit.");
                while (!cancelled)
                {
                    consumer.Poll(TimeSpan.FromMilliseconds(100));
                }
            }
        }

        /// <summary>
        ///     在这实例中
        ///         - offsets 是手动提交的。
        ///         - consumer.Consume方法用于消费消息
        ///             (所有其他事件仍由事件处理程序处理)
        ///         -没有为了 轮询（消耗）循环 创建额外的线程。
        /// </summary>
        public static void Run_Consume(string brokerList, List<string> topics)
        {
            var config = new Dictionary<string, object>
            {
                { "bootstrap.servers", brokerList },
                { "group.id", "csharp-consumer" },
                { "enable.auto.commit", false },
                { "statistics.interval.ms", 60000 },
                { "session.timeout.ms", 6000 },
                { "auto.offset.reset", "smallest" }
            };

            using (var consumer = new Consumer<Ignore, string>(config, null, new StringDeserializer(Encoding.UTF8)))
            {
                // 注意:所有事件处理都是在主线程中处理的，也就是说同步的

                consumer.OnPartitionEOF += (_, end)
                    => Console.WriteLine("Reached end of topic "+end.Topic+" partition "+end.Partition+", next message will be at offset "+end.Offset);

                consumer.OnError += (_, error)=> Console.WriteLine("Error: "+error);

                // 当反序列化有错误，或者消费的过程中发生了错误，即error != NoError，就会触发该事件
                consumer.OnConsumeError += (_, error)=> Console.WriteLine("Consume error: "+error);

                // 当消费者被分配一组新的分区时触发该事件
                consumer.OnPartitionsAssigned += (_, partitions) =>
                {
                    Console.WriteLine("Assigned partitions:"+partitions+"  member id: "+consumer.MemberId);
                    // 如果您未向OnPartitionsAssigned事件添加处理程序，则会自动执行以下.Assign调用。 如果你这样做，你必须明确地调用.Assign以便消费者开始消费消息。
                    //开始从分区中消息消息
                    consumer.Assign(partitions);
                };

                // 当消费者的当前分区集已被撤销时引发该事件。
                consumer.OnPartitionsRevoked += (_, partitions) =>
                {
                    Console.WriteLine("Revoked partitions:"+partitions);
                    // 如果您未向OnPartitionsRevoked事件添加处理程序，则下面的.Unassign调用会自动发生。 如果你这样做了，你必须明确地调用.Usessign以便消费者停止从它先前分配的分区中消费消息。

                    //停止从分区中消费消息
                    consumer.Unassign();
                };

                consumer.OnStatistics += (_, json) => Console.WriteLine("Statistics: "+json);

                consumer.Subscribe(topics);

                Console.WriteLine("Started consumer, Ctrl-C to stop consuming");

                var cancelled = false;
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // 防止进程退出
                    cancelled = true;
                };

                while (!cancelled)
                {
                    if (!consumer.Consume(out Message<Ignore, string> msg, TimeSpan.FromMilliseconds(100)))
                    {
                        continue;
                    }

                    Console.WriteLine("Topic: "+msg.Topic+" Partition: "+msg.Partition+" Offset: "+msg.Offset+" "+msg.Value);

                    if (msg.Offset % 5 == 0)
                    {
                        var committedOffsets = consumer.CommitAsync(msg).Result;
                        Console.WriteLine("Committed offset: "+committedOffsets);
                    }
                }
            }
        }

        /// <summary>
        ///     在这个例子中
        ///         - 消费者组功能（即.Subscribe +offset提交）不被使用。
        ///         - 将消费者手动分配给分区，并始终从特定偏移量（0）开始消耗。
        /// </summary>
        public static void Run_ManualAssign(string brokerList, List<string> topics)
        {
            var config = new Dictionary<string, object>
            {
                // 即使您不打算使用任何使用者组功能，也必须在创建使用者时指定group.id属性。
                { "group.id", new Guid().ToString() },
                { "bootstrap.servers", brokerList },
                // 即使消费者没有订阅该组，也可以将分区偏移量提交给一个组。 在这个例子中，自动提交被禁用以防止发生这种情况。
                { "enable.auto.commit", false }
            };

            using (var consumer = new Consumer<Ignore, string>(config, null, new StringDeserializer(Encoding.UTF8)))
            {
                //总是从0开始消费
                consumer.Assign(topics.Select(topic => new TopicPartitionOffset(topic, 0, Offset.Beginning)).ToList());

                // 引发严重错误，例如 连接失败或所有Kafka服务器失效。
                consumer.OnError += (_, error) => Console.WriteLine("Error: "+error);

                // 这个事件是由于在反序列化出现错误，或者在消息消息的时候出现错误，也就是 error != NoError 的时候引发该事件
                consumer.OnConsumeError += (_, error) => Console.WriteLine("Consume error: "+error);

                while (true)
                {
                    if (consumer.Consume(out Message<Ignore, string> msg, TimeSpan.FromSeconds(1)))
                    {
                        Console.WriteLine("Topic: "+msg.Topic+" Partition: "+msg.Partition+" Offset: "+msg.Offset+" "+msg.Value);
                    }
                }
            }
        }

        private static void PrintUsage()=> Console.WriteLine("Usage: .. <poll|consume|manual> <broker,broker,..> <topic> [topic..]");

        public static void Main(string[] args)
        {
            if (args.Length < 3)
            {
                //PrintUsage();
                //return;
            }

            var mode = "consume";
            var brokerList = "192.168.2.3:9092";
            var topics = "news".Split(',').ToList();

            switch (mode)
            {
                case "poll":
                    Run_Poll(brokerList, topics);
                    break;
                case "consume":
                    Run_Consume(brokerList, topics);
                    break;
                case "manual":
                    Run_ManualAssign(brokerList, topics);
                    break;
                default:
                    PrintUsage();
                    break;
            }
        }
    }
}