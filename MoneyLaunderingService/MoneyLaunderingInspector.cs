
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using System.Net;

public class MoneyLaunderingInspector : IHostedService
{
    private IConsumer<Ignore, string> consumer;
    private IProducer<Null, string> producer;
    private static readonly string IncomingMoneyTopic = "incoming-money-topic";
    private static readonly string InspectionResultTopic = "inspection-result-topic";

    public MoneyLaunderingInspector()
    {
        string bootstrapServers = "localhost:29092";

        ConsumerConfig config1 = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = "launderingInspector",
            AutoOffsetReset = new AutoOffsetReset?(AutoOffsetReset.Earliest)
        };

        this.consumer = new ConsumerBuilder<Ignore, string>(config1).Build();
        this.consumer.Subscribe(IncomingMoneyTopic);

        ProducerConfig config2 = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            ClientId = Dns.GetHostName()
        };

        this.producer = new ProducerBuilder<Null, string>(config2).Build();
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (true)
            {
                try
                {
                    var result = this.consumer.Consume();
                    Console.WriteLine($"Received money: { result.Message.Value} Euro");

                    if (double.TryParse(result.Message.Value, out double amount))
                    {
                        string value = amount < 1000 ? "True" : "False";
                        await producer.ProduceAsync(InspectionResultTopic, new Message<Null, string>() {  Value = value ?? "Error" });
                        Console.WriteLine("Payment accepted status: " + value);
                    }
                }
                catch (ProduceException<Null, string> ex)
                {
                    Console.WriteLine("Delivery failed: " + ex.Error.Reason);
                }
                catch (ConsumeException ex)
                {
                    Console.WriteLine("Error occured: " + ex.Error.Reason);
                }
            }
        }
        catch (OperationCanceledException ex)
        {
            this.producer.Flush(TimeSpan.FromSeconds(10.0));
            this.consumer.Close();
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        this.producer.Dispose();
        this.consumer.Dispose();
        return Task.CompletedTask;
    }
}
