using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using System.Threading;
using Confluent.Kafka;

namespace LockerRoomChatArchiveService
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            InitializeComponent();
            StartService();
        }

        public void StartService()
        {
            var conf = new ConsumerConfig
            {
                GroupId = "chat-archive.candidate-5",
                BootstrapServers = "kafka+ssl://ec2-18-204-47-99.compute-1.amazonaws.com:9096,kafka+ssl://ec2-18-233-140-74.compute-1.amazonaws.com:9096,kafka+ssl://ec2-18-208-61-56.compute-1.amazonaws.com:9096,kafka+ssl://ec2-34-224-229-106.compute-1.amazonaws.com:9096,kafka+ssl://ec2-18-233-211-98.compute-1.amazonaws.com:9096,kafka+ssl://ec2-34-235-216-30.compute-1.amazonaws.com:9096,kafka+ssl://ec2-52-204-144-208.compute-1.amazonaws.com:9096,kafka+ssl://ec2-34-203-24-91.compute-1.amazonaws.com:9096",
                // Note: The AutoOffsetReset property determines the start offset in the event
                // there are not yet any committed offsets for the consumer group for the
                // topic/partitions of interest. By default, offsets are committed
                // automatically, so in this example, consumption will only start from the
                // earliest message in the topic 'my-topic' the first time you run the program.
                AutoOffsetReset = AutoOffsetReset.Earliest,
                SecurityProtocol = SecurityProtocol.Ssl,
                Debug = "security,broker,consumer",
                SslCertificateLocation = @"C:\certs\white-60788_client.crt",
                SslKeyLocation = @"C:\certs\white-60788_client.key",
                SslCaLocation = @"C:\certs\lockerroom-trusted-ca.crt"
            };

            using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                c.Subscribe("white-60788.Chat");

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            System.Diagnostics.Debug.WriteLine($"Start consuming");
                            var cr = c.Consume(cts.Token);
                            System.Diagnostics.Debug.WriteLine($"Consumed message '{cr.Message.Value}' at: '{cr.TopicPartitionOffset}'.");
                            Console.WriteLine($"Consumed message '{cr.Message.Value}' at: '{cr.TopicPartitionOffset}'.");
                        }
                        catch (ConsumeException e)
                        {
                            System.Diagnostics.Debug.WriteLine($"Error occured: {e.Error.Reason}");
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    c.Close();
                }
            }
        }
    }
}
