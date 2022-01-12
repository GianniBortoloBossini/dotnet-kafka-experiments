var config = new ProducerConfig{
    BootstrapServers = "localhost:29093",
    ClientId = Dns.GetHostName()
};

using(var producer = new ProducerBuilder<Null, string>(config).Build())
{
    try
    {
        var dr = await producer.ProduceAsync("user-added", new Message<Null, string> { Value = $"Sent message at {DateTime.Now}" });
        Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
    }
    catch (ProduceException<Null, string> e)
    {
        Console.WriteLine($"Delivery failed: {e.Error.Reason}");
    }
}