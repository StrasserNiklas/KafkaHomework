using Confluent.Kafka;
using Microsoft.Extensions.Hosting;

public class TransactionVisualizer : IHostedService
{
    private IConsumer<Ignore, string> consumer;
    private static readonly string InspectionResultTopic = "inspection-result-topic";
    private int acceptedTransactions = 0;
    private int declinedTransactions = 0;

    public TransactionVisualizer()
    {
        string str = "localhost:29092";

        var config = new ConsumerConfig
        {
            BootstrapServers = str,
            GroupId = "launderingVisualizer",
            AutoOffsetReset = new AutoOffsetReset?(AutoOffsetReset.Earliest)
        };
        this.consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        this.consumer.Subscribe(InspectionResultTopic);
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (true)
            {
                try
                {
                    bool accepted;

                    if (bool.TryParse(this.consumer.Consume().Message.Value, out accepted))
                    {
                        if (accepted)
                        {
                            this.acceptedTransactions++;
                        }
                            
                        else
                        {
                            this.declinedTransactions++;
                        }

                        Console.WriteLine($"Accepted transactions: {this.acceptedTransactions}. Declined transactions: {this.declinedTransactions}");
                    }
                }
                catch (ConsumeException ex)
                {
                    Console.WriteLine("Error occured: " + ex.Error.Reason);
                }
            }
        }
        catch (OperationCanceledException ex)
        {
            this.consumer.Close();
            return Task.CompletedTask;
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        this.consumer.Dispose();
        return Task.CompletedTask;
    }
}