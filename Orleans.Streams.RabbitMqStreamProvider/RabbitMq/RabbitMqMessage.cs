using System.Collections.Generic;

namespace Orleans.Streams.RabbitMq
{
    public class RabbitMqMessage
    {
        public byte[] Body { get; set; }

        public string Exchange { get; set; }

        public string RoutingKey { get; set; }

        // Publish only property:
        public bool ShouldConfirmPublish { get; set; } = true;

        // Basic properties:
        public string AppId { get; set; }
        public string ClusterId { get; set; }
        public string ContentEncoding { get; set; }
        public string ContentType { get; set; }
        public string CorrelationId { get; set; }
        public string Expiration { get; set; }
        public IDictionary<string, object> Headers { get; set; }
        public string MessageId { get; set; }
        public bool Persistent { get; set; }
        public byte? Priority { get; set; }
        public string ReplyTo { get; set; }
        public long? Timestamp { get; set; }
        public string Type { get; set; }
        public string UserId { get; set; }
    }
}