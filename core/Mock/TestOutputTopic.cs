using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock.Pipes;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace Streamiz.Kafka.Net.Mock
{
    /// <summary>
    /// <see cref="TestOutputTopic{K, V}" /> is used to read records from a topic in <see cref="TopologyTestDriver"/> and it's NOT THREADSAFE.
    /// To use <see cref="TestOutputTopic{K, V}" /> create a new instance via
    /// <see cref="TopologyTestDriver.CreateOuputTopic{K, V}(string)"/>.
    /// In actual test code, you can read record values, keys, keyvalue or list of keyvalue.
    /// If you have multiple source topics, you need to create a <see cref="TestOutputTopic{K, V}" /> for each.
    /// <example>
    /// Processing records
    /// <code>
    /// var outputTopic = builder.CreateOuputTopic&lt;string, string&gt;("test-output", TimeSpan.FromSeconds(5));
    /// var kv = outputTopic.ReadKeyValue();
    /// DO ASSERT HERE
    /// </code>
    /// </example>
    /// </summary>
    /// <typeparam name="K">key type</typeparam>
    /// <typeparam name="V">value type</typeparam>
    public class TestOutputTopic<K, V>
    {
        private readonly IPipeOutput pipe;
        private readonly IStreamConfig configuration;
        private readonly ISerDes<K> keySerdes;
        private readonly ISerDes<V> valueSerdes;
        private readonly ILogger log = Logger.GetLogger(typeof(TestOutputTopic<K, V>));

        private TestOutputTopic()
        {

        }

        internal TestOutputTopic(IPipeOutput pipe, IStreamConfig configuration, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            this.pipe = pipe;
            this.configuration = configuration;
            this.keySerdes = keySerdes;
            this.valueSerdes = valueSerdes;

            if (this.keySerdes != null)
                this.keySerdes.Initialize(new SerDesContext(configuration));
            if (this.valueSerdes != null)
                this.valueSerdes.Initialize(new SerDesContext(configuration));
        }

        internal IPipeOutput Pipe => pipe;

        /// <summary>
        /// Verify if the topic queue is empty.
        /// </summary>
        public bool IsEmpty => pipe.IsEmpty;

        /// <summary>
        /// Get size of unread record in the topic queue.
        /// </summary>
        public int QueueSize => pipe.Size;

        private TestRecord<K, V> ReadRecord()
        {
            try
            {
                var record = pipe.Read();

                var key = keySerdes != null ?
                    keySerdes.Deserialize(record.Key, new SerializationContext(MessageComponentType.Key, pipe.TopicName)) :
                    (K)configuration.DefaultKeySerDes.DeserializeObject(record.Key, new SerializationContext(MessageComponentType.Key, pipe.TopicName));

                var value = valueSerdes != null ?
                    valueSerdes.Deserialize(record.Value, new SerializationContext(MessageComponentType.Value, pipe.TopicName)) :
                    (V)configuration.DefaultValueSerDes.DeserializeObject(record.Value, new SerializationContext(MessageComponentType.Value, pipe.TopicName));

                return new TestRecord<K, V> { Key = key, Value = value };
            }catch(StreamsException e)
            {
                log.LogWarning(e, "{Message}", e.Message);
                return null;
            }
        }

        #region Read 

        /// <summary>
        /// Read one record from the output topic and return record's value.
        /// </summary>
        /// <returns>Next value for output topic.</returns>
        public V ReadValue() => this.ReadRecord().Value;

        /// <summary>
        /// Read one record from the output topic and return its key and value as pair.
        /// </summary>
        /// <returns>Next output as <see cref="ConsumeResult{TKey, TValue}"/></returns>
        public ConsumeResult<K, V> ReadKeyValue()
        {
            var r = this.ReadRecord();

            return 
                r != null ? new ConsumeResult<K, V>{
                                Message = new Message<K, V> { Key = r.Key, Value = r.Value, Timestamp = new Timestamp(r.Timestamp.HasValue ? r.Timestamp.Value : DateTime.Now) }
                            } 
                        : null;
        }

        #endregion

        #region Read List

        /// <summary>
        /// Read all records from topic to List.
        /// </summary>
        /// <returns>List of output records.</returns>
        public IEnumerable<ConsumeResult<K, V>> ReadKeyValueList()
        {
            List<ConsumeResult<K, V>> records = new List<ConsumeResult<K, V>>();
            foreach(var record in pipe.ReadList())
            {
                var key = record.Key != null ?
                    (keySerdes != null ? 
                        keySerdes.Deserialize(record.Key, new SerializationContext(MessageComponentType.Key, pipe.TopicName)) :
                        (K)configuration.DefaultKeySerDes.DeserializeObject(record.Key, new SerializationContext(MessageComponentType.Key, pipe.TopicName))) 
                    : default;

                var value = record.Value != null ?
                    (valueSerdes != null ?
                        valueSerdes.Deserialize(record.Value, new SerializationContext(MessageComponentType.Value, pipe.TopicName)) :
                        (V)configuration.DefaultValueSerDes.DeserializeObject(record.Value, new SerializationContext(MessageComponentType.Value, pipe.TopicName))) 
                    : default;

                records.Add(new ConsumeResult<K, V>{
                    Message = new Message<K, V> { Key = key, Value = value, Timestamp = new Timestamp(DateTime.Now) }
                });
            }
            return records;
        }

        /// <summary>
        /// Read all values from topic to List.
        /// </summary>
        /// <returns>List of output values.</returns>
        public IEnumerable<V> ReadValueList()
            => ReadKeyValueList().Select(kv => kv.Message.Value).ToList();

        #endregion
    }
}