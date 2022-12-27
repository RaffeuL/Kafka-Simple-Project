

using Confluent.Kafka;

class Program
{
    public static void Main(string[] args)
    {
        var conf = new ConsumerConfig
        {
            GroupId = "test-topic-group",
            BootstrapServers = "localhost:9092",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var c = new ConsumerBuilder<Ignore, string>(conf).Build();
        {
            c.Subscribe("test-topic");

            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            try
            {
                while (true)
                {
                    try
                    {
                        var cr = c.Consume(cts.Token);
                        Console.WriteLine($"Consumed message {cr.Value} at {cr.TopicPartitionOffset}");
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error ocurred: {e.Error.Reason}");
                    }

                    Thread.Sleep(1000);
                }
            }
            catch (OperationCanceledException)
            {
                c.Close();
            }
        }
    }
}