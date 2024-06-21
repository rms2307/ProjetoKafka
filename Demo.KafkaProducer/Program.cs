using Confluent.Kafka;
using Demo.Kafka;
using Microsoft.AspNetCore.Mvc;
using System.Text;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.MapPost("/producer", ([FromServices] IConfiguration configuration, TodoItem todoItem) =>
{
    string message = JsonSerializer.Serialize(todoItem);

    try
    {
        byte[] bodyMessage = Encoding.UTF8.GetBytes(message);
        Console.WriteLine($"Dados: {todoItem}");

        string topicName = configuration["Kafka_Topic"]!;
        ProducerConfig configKafka = new()
        {
            BootstrapServers = configuration["Kafka_Broker"]
        };

        using (IProducer<int, string> producer = new ProducerBuilder<int, string>(configKafka).Build())
        {
            var result = producer.ProduceAsync(
                        topicName,
                        new Message<int, string>
                        {
                            Key = todoItem.Id,
                            Value = message
                        }).Result;

            Console.WriteLine($"Apache Kafka - Envio para o tópico {topicName} do Apache Kafka concluído | " +
                        $"{message} | Status: {result.Status}");
        }
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"Exceção: {ex.GetType().FullName} | " +
                             $"Mensagem: {ex.Message}");
    }

    return Results.Created(string.Empty, todoItem);
})
.WithName("PostTopic")
.WithOpenApi();

app.Run();
