namespace Streamiz.Kafka.Net.Table.Internal
{
    internal class Change<T>
    {
        public T OldValue { get; }
        public T NewValue { get; }

        public Change(T old, T @new)
        {
            this.OldValue = old;
            this.NewValue = @new;
        }

        public override string ToString() => $"OldValue:{OldValue}|NewValue:{NewValue}";
    }
}
