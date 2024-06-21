using Confluent.Kafka;

namespace Demo.KafkaConsumer
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IConfiguration _configuration;

        public Worker(ILogger<Worker> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            string topicName = _configuration["Kafka_Topic"]!;

            var config = new ConsumerConfig
            {
                BootstrapServers = _configuration["Kafka_Broker"],
                GroupId = $"{topicName}-group-0",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            try
            {
                using (IConsumer<Ignore, string> consumer = new ConsumerBuilder<Ignore, string>(config).Build())
                {
                    consumer.Subscribe(topicName);
                    try
                    {
                        while (!stoppingToken.IsCancellationRequested)
                        {
                            _logger.LogInformation($"Buscando mensagens do topic: {topicName}...");

                            ConsumeResult<Ignore, string> cr = consumer.Consume();

                            _logger.LogInformation($"Mensagem lida: {cr.Message.Value}");
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError($"Erro: {ex.Message}");
                        consumer.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Erro ao processar mensagem: {ex.Message}.");
            }
        }
    }
}
