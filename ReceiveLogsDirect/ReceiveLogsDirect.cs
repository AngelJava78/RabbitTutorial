using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Linq;

namespace ReceiveLogsDirect
{
    class ReceiveLogsDirect
    {
        static void Main(string[] args)
        {
            Console.WriteLine("ReceiveLogsDirect program");
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare(exchange: "direct_logs", type: "direct");
                    //declare a server-named queue
                    var queueName = channel.QueueDeclare(queue: "").QueueName;
                    if (args.Length < 1)
                    {
                        Console.WriteLine($"Usage: {Environment.GetCommandLineArgs()[0]} [info] [warning] [error]");
                        Console.WriteLine("Press any key to exit.");
                        Console.ReadKey();
                        Environment.ExitCode = 1;
                        return;
                    }
                    foreach(var severity in args)
                    {
                        channel.QueueBind(queue: queueName, exchange: "direct_logs", routingKey: severity);
                    }

                    Console.WriteLine(" [*] Waiting for messages.");

                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);
                        var routingKey = ea.RoutingKey;
                        Console.WriteLine($" [x] Received '{routingKey}': '{message}'");
                    };
                    channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);

                    Console.WriteLine("Press any key to exit...");
                    Console.ReadKey();

                }
            }
        }
    }
}
