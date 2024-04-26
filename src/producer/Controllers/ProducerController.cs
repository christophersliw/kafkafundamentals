using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using producer.Models;
using producer.Services;

namespace producer.Controllers;

[Route("api/[controller]")]
[ApiController]
public class ProducerController : ControllerBase
{
    private readonly ProducerService _producerService;

    public ProducerController(ProducerService producerService)
    {
        _producerService = producerService;
    }
    
    [HttpPost]
    public async Task<IActionResult> Produce([FromBody] ProducerRequest request)
    {
        var message = JsonSerializer.Serialize(request);

        await _producerService.ProduceAsync("test-topic1", message);

        return Ok("Message sent to Kafka...");
    }
}