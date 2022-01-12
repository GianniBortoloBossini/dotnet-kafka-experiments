using Confluent.Kafka;

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:29092",
    GroupId = "consumer",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

CancellationTokenSource source = new CancellationTokenSource();

var keepConsuming = true;
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true; // prevent the process from terminating.
    keepConsuming = false;
    Console.WriteLine("Stopping...");
    source.Cancel();
};

using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
{
    try
    {
        consumer.Subscribe("user-added");
        while (keepConsuming)
        {
            try
            {
                var consumeResult = consumer.Consume(source.Token);

                Console.WriteLine($"Consumed message '{consumeResult.Message.Value}' at: '{consumeResult.TopicPartitionOffset}'.");
                //process in here
                consumer.Commit(consumeResult);
                Console.WriteLine("Committed offset.");
            }
            catch (ConsumeException e)
            {
                Console.WriteLine($"Error occurred: {e.Error.Reason}");

            }
            catch (OperationCanceledException e)
            {
                Console.WriteLine($"Consumption failed: {e}");
            }
            finally
            {

            }
        }
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine("Operation canceled.");
    }
    finally
    {
        Console.WriteLine("Closing consumer.");
        consumer.Close();
    }
}