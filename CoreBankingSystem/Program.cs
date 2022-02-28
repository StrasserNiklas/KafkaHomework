using Confluent.Kafka;
using System.Net;

string IncomingMoneyTopic = "incoming-money-topic";
ProducerConfig producerConfig = new ProducerConfig()
{
     BootstrapServers = "localhost:29092",
     ClientId = Dns.GetHostName()
};

using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();

while (true)
{
    Console.Write("\nEnter the amount of money to send:");
    string input = Console.ReadLine();

    try
    {
        DeliveryResult<Null, string> delivery = await producer.ProduceAsync(IncomingMoneyTopic, new Message<Null, string>() {  Value = input ?? "Error" });
        Console.WriteLine("Delivered " + delivery.Value + " Euros.");
    }
    catch (ProduceException<Null, string> ex)
    {
        Console.WriteLine("Delivery failed: " + ex.Error.Reason);
    }
}

