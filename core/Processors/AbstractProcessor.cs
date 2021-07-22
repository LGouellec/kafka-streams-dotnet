using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Logging;

namespace Streamiz.Kafka.Net.Processors
{
    internal abstract class AbstractProcessor<K, V> : IProcessor<K, V>
    {
        protected ILogger log = null;
        protected string logPrefix = "";

        public ProcessorContext Context { get; protected set; }

        public string Name { get; set; }
        public IList<string> StateStores { get; protected set; }

        public ISerDes<K> KeySerDes => Key is ISerDes<K> ? (ISerDes<K>)Key : null;

        public ISerDes<V> ValueSerDes => Value is ISerDes<V> ? (ISerDes<V>)Value : null;

        public ISerDes Key { get; internal set; } = null;

        public ISerDes Value { get; internal set; } = null;

        public IList<IProcessor> Next { get; private set; } = new List<IProcessor>();

        #region Ctor

        protected AbstractProcessor()
            : this(null)
        {

        }

        protected AbstractProcessor(string name)
            : this(name, null, null)
        {
        }

        protected AbstractProcessor(string name, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
            : this(name, keySerdes, valueSerdes, null)
        {
        }

        protected AbstractProcessor(string name, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, List<string> stateStores)
        {
            Name = name;
            Key = keySerdes;
            Value = valueSerdes;
            StateStores = stateStores != null ? new List<string>(stateStores) : new List<string>();
            log = Logger.GetLogger(GetType());
        }

        #endregion

        public virtual void Close()
        {
            foreach (var n in Next)
            {
                n.Close();
            }
        }

        #region Forward

        public virtual void Forward<K1, V1>(K1 key, V1 value, long ts)
        {
            Context.ChangeTimestamp(ts);
            Forward<K1, V1>(key, value);
        }

        public virtual void Forward<K1, V1>(K1 key, V1 value)
        {
            log.LogDebug("{LogPrefix}Forward<{KeyType},{ValueType}> message with key {Key} and value {Value} to each next processor",
                logPrefix, typeof(K1).Name, typeof(V1).Name, key, value);
            foreach (var n in Next)
                if (n is IProcessor<K1, V1>)
                    (n as IProcessor<K1, V1>).Process(key, value);
        }

        public virtual void Forward<K1, V1>(K1 key, V1 value, string name)
        {
            foreach (var n in Next)
            {
                if (n is IProcessor<K1, V1> && n.Name.Equals(name))
                {
                    log.LogDebug(
                        "{LogPrefix}Forward<{KeyType},{ValueType}> message with key {Key} and value {Value} to processor {Processor}", logPrefix,
                        typeof(K1).Name, typeof(V1).Name, key, value, name);
                    (n as IProcessor<K1, V1>).Process(key, value);
                }
            }
        }

        public virtual void Forward(K key, V value)
        {
            log.LogDebug("{LogPrefix}Forward<{KeyType},{ValueType}> message with key {Key} and value {Value} to each next processor",
                logPrefix, typeof(K).Name, typeof(V).Name, key, value);
            foreach (var n in Next)
            {
                if (n is IProcessor<K, V>)
                    (n as IProcessor<K, V>).Process(key, value);
                else
                    n.Process(key, value);
            }
        }

        public virtual void Forward(K key, V value, string name)
        {
            foreach (var n in Next)
            {
                if (n.Name.Equals(name))
                {
                    log.LogDebug(
                        "{LogPrefix}Forward<{KeyType},{ValueType}> message with key {Key} and value {Value} to processor {Processor}", logPrefix,
                        typeof(K).Name, typeof(V).Name, key, value, name);
                    if (n is IProcessor<K, V>)
                        (n as IProcessor<K, V>).Process(key, value);
                    else
                        n.Process(key, value);
                }
            }
        }

        #endregion

        public virtual void Init(ProcessorContext context)
        {
            log.LogDebug("{LogPrefix}Initializing process context", logPrefix);
            Context = context;
            foreach (var n in Next)
            {
                n.Init(context);
            }
            log.LogDebug("{LogPrefix}Process context initialized", logPrefix);
        }

        protected void LogProcessingKeyValue(K key, V value) => log.LogDebug(
            "{LogPrefix}Process<{KeyType},{ValueType}> message with key {Key} and {Value} with record metadata [topic:{Topic}|partition:{Partition}|offset:{Offset}]",
            logPrefix, typeof(K).Name, typeof(V).Name, key, value, Context.RecordContext.Topic,
            Context.RecordContext.Partition, Context.RecordContext.Offset);

        #region Setter

        internal void SetTaskId(TaskId id)
        {
            logPrefix = $"stream-task[{id.Id}|{id.Partition}]|processor[{Name}]- ";
        }

        public void AddNextProcessor(IProcessor next)
        {
            if (next != null && !Next.Contains(next as IProcessor))
                Next.Add(next);
        }

        #endregion

        #region Process object

        public void Process(ConsumeResult<byte[], byte[]> record)
        {
            bool throwException = false;
            ObjectDeserialized key = null;
            ObjectDeserialized value = null;

            if (KeySerDes != null)
            {
                key = DeserializeKey(record);
                if (key.MustBeSkipped)
                {
                    log.LogDebug(
                        "{LogPrefix} Message with record metadata [topic:{Topic}|partition:{Partition}|offset:{Offset}] was skipped !",
                        logPrefix, Context.RecordContext.Topic, Context.RecordContext.Partition,
                        Context.RecordContext.Offset);
                    return;
                }
            }
            else
                throwException = true;

            if (ValueSerDes != null)
            {
                value = DeserializeValue(record);
                if (value.MustBeSkipped)
                {
                    log.LogDebug(
                        "{LogPrefix} Message with record metadata [topic:{Topic}|partition:{Partition}|offset:{Offset}] was skipped !",
                        logPrefix, Context.RecordContext.Topic, Context.RecordContext.Partition,
                        Context.RecordContext.Offset);
                    return;
                }
            }
            else
                throwException = true;

            if (throwException)
            {
                var s = KeySerDes == null ? "key" : "value";
                log.LogError(
                    "{LogPrefix}Impossible to receive source data because keySerdes and/or valueSerdes is not setted ! KeySerdes : {KeySerdes} | ValueSerdes : {ValueSerdes}",
                    logPrefix, KeySerDes != null ? KeySerDes.GetType().Name : "NULL",
                    ValueSerDes != null ? ValueSerDes.GetType().Name : "NULL");
                throw new StreamsException($"{logPrefix}The {s} serdes is not compatible to the actual {s} for this processor. Change the default {s} serdes in StreamConfig or provide correct Serdes via method parameters(using the DSL)");
            }
            else
                Process(key.Bean, value.Bean);
        }

        public void Process(object key, object value)
        {
            if((key == null || key is K) && (value == null || value is V))
                Process((K)key, (V)value);
        }

        #endregion

        #region Abstract

        public abstract void Process(K key, V value);

        #endregion

        public override bool Equals(object obj)
        {
            return obj is AbstractProcessor<K, V> && ((AbstractProcessor<K, V>)obj).Name.Equals(Name);
        }

        public override int GetHashCode()
        {
            return Name.GetHashCode();
        }

        public virtual ObjectDeserialized DeserializeKey(ConsumeResult<byte[], byte[]> record)
        {
            try
            {
                var o = Key.DeserializeObject(record.Message.Key, new SerializationContext(MessageComponentType.Key, record.Topic, record.Message.Headers));
                return new ObjectDeserialized(o, false);
            }catch(Exception e)
            {
                var handlerResponse = Context.Configuration.DeserializationExceptionHandler != null ?
                    Context.Configuration.DeserializationExceptionHandler(Context, record, e) : ExceptionHandlerResponse.FAIL;

                if (handlerResponse == ExceptionHandlerResponse.FAIL)
                    throw new DeserializationException($"{ logPrefix }Error during key deserialization[Topic:{ record.Topic}| Partition:{ record.Partition}| Offset:{ record.Offset}| Timestamp:{ record.Message.Timestamp.UnixTimestampMs}]", e);
                else
                {
                    StringBuilder sb = new StringBuilder();
                    sb.AppendLine($"{logPrefix}Error during key deserialization [Topic:{record.Topic}|Partition:{record.Partition}|Offset:{record.Offset}|Timestamp:{record.Message.Timestamp.UnixTimestampMs}] with exception {e}.");
                    sb.AppendLine($"{logPrefix}DeserializationExceptionHandler return 'CONTINUE', so this message will be skipped and not processed !");
                    log.LogError(sb.ToString());
                    return ObjectDeserialized.ObjectSkipped;
                }
            }

        }

        public virtual ObjectDeserialized DeserializeValue(ConsumeResult<byte[], byte[]> record)
        {
            try
            {
                var o = Value.DeserializeObject(record.Message.Value, new SerializationContext(MessageComponentType.Value, record.Topic, record.Message.Headers));
                return new ObjectDeserialized(o, false);
            }
            catch (Exception e)
            {
                var handlerResponse = Context.Configuration.DeserializationExceptionHandler != null ?
                    Context.Configuration.DeserializationExceptionHandler(Context, record, e) : ExceptionHandlerResponse.FAIL;

                if (handlerResponse == ExceptionHandlerResponse.FAIL)
                    throw new DeserializationException($"{ logPrefix }Error during value deserialization[Topic:{ record.Topic}| Partition:{ record.Partition}| Offset:{ record.Offset}| Timestamp:{ record.Message.Timestamp.UnixTimestampMs}]", e);
                else
                {
                    StringBuilder sb = new StringBuilder();
                    sb.AppendLine($"{logPrefix}Error during value deserialization [Topic:{record.Topic}|Partition:{record.Partition}|Offset:{record.Offset}|Timestamp:{record.Message.Timestamp.UnixTimestampMs}] with exception {e}.");
                    sb.AppendLine($"{logPrefix}DeserializationExceptionHandler return 'CONTINUE', so this message will be skipped and not processed !");
                    log.LogError(sb.ToString());
                    return ObjectDeserialized.ObjectSkipped;
                }
            }
        }
    }
}
