namespace Demo.Kafka
{
    public record TodoItem
    {
        public required int Id { get; set; }
        public required string Name { get; set; }
        public required bool IsComplete { get; set; }
    }

}
