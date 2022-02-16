using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ReceiveLogsTopic
{
    class ReceiveLogsTopic
    {
        static void Main(string[] args)
        {
            Console.WriteLine("ReceiveLogsTopic program");
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare(exchange: "topic_logs", type: "topic");
                    var queueName = channel.QueueDeclare().QueueName;

                    if(args.Length< 1)
                    {
                        Console.WriteLine($"Usage: {Environment.GetCommandLineArgs()[0]} [binding_key..]");
                        Console.WriteLine("Press any key to continue...");
                        Console.ReadKey();
                        Environment.ExitCode = 1;
                        return;
                    }

                    foreach(var bindingKey in args)
                    {
                        channel.QueueBind(queue: queueName,
                            exchange: "topic_logs",
                            routingKey: bindingKey
                            );

                    }

                    Console.WriteLine(" [*] Waiting for messages. To exit press CTRL + C");

                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body.ToArray();
                        var routingKey = ea.RoutingKey;
                        var message = Encoding.UTF8.GetString(body);
                        Console.WriteLine($" [x] Received '{routingKey}': '{message}'");
                    };
                    channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);

                    Console.WriteLine("Press any key to exit.");
                    Console.ReadKey();
                }
            }
        }
    }
}
