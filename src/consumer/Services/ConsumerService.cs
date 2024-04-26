using Confluent.Kafka;

namespace consumer.Services;

public class ConsumerService : BackgroundService
{
    private readonly IConsumer<Ignore, string> _consumer;
    private readonly ILogger<ConsumerService> _logger;

    public ConsumerService(IConfiguration configuration, ILogger<ConsumerService> logger)
    {
        _logger = logger;
        
        _logger.LogInformation("ConsumerService - Constructor - start");
        
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = configuration["Kafka:BootstrapServers"],
            GroupId = "TestGroup1",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        _consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
        
        _logger.LogInformation("ConsumerService - Constructor - end");

    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("ConsumerService - ExecuteAsync - start");
        
        _consumer.Subscribe("test-topic1");
        
        _logger.LogInformation("ConsumerService - ExecuteAsync - subcribed to test-topic1");

        while (!stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("ConsumerService - ExecuteAsync - receiving message...");
            ProcessKafkaMessage(stoppingToken);
            Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
        }

        _consumer.Close();
    }

    public void ProcessKafkaMessage(CancellationToken stoppingToken)
    {
        try
        {
            var consumeResult = _consumer.Consume(stoppingToken);
            var message = consumeResult.Message.Value;

            _logger.LogInformation($"Received inventory update: {message}");
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error processing Kafka message: {ex.Message}");
        }
    }
}