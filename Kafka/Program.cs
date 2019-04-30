using System;
using System.IO;
using System.Text;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka.Examples.Producer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            //if (args.Length != 2)
            //{
                //Console.WriteLine("Usage: .. brokerList topicName");
                //return;
            //}

            string brokerList = "192.168.2.3:9092";
            string topicName = "news";
            string message = "我就是要传输的消息内容";
            try{
                //这是以异步方式生产消息的代码实例
                var config = new Dictionary<string, object> { { "bootstrap.servers", brokerList } };
                using (var producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8)))
                {
                    Console.WriteLine("...");
                    for(int i=0;i<1000;i++){
                        var deliveryReport = producer.ProduceAsync(topicName, null, message);
                        deliveryReport.ContinueWith(task =>
                        {
                            Console.WriteLine("Producer: " + producer.Name + "\r\nTopic: " + topicName + "\r\nPartition: " + task.Result.Partition + "\r\nOffset: " + task.Result.Offset);
                        });
                    }
                    producer.Flush(TimeSpan.FromSeconds(10));
                    Console.WriteLine(".2..");

                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }
    }
}