using RabbitMQ.Client;
using System;
using System.Text;

namespace Send
{
    public class Send
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Send program");
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using(var connection = factory.CreateConnection())
            {
                using(var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queue: "hello",
                        durable: false,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null
                        );
                    string message = "Hello world!";
                    var body = Encoding.UTF8.GetBytes(message);
                    channel.BasicPublish(exchange: "",
                        routingKey: "hello",
                        basicProperties: null,
                        body: body
                        );
                    Console.WriteLine($" [x] Sent {message}");
                }
            }
            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
           
        }
    }
}
