namespace Streamiz.Kafka.Net.Stream.Internal
{
    internal interface INameProvider
    {
        string NewProcessorName(string prefix);

        string NewStoreName(string prefix);
    }
}
