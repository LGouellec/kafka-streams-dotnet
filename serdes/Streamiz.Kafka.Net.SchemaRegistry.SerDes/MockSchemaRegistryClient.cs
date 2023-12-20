using Confluent.SchemaRegistry;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Streamiz.Kafka.Net.SchemaRegistry.SerDes.Mock
{
    /// <summary>
    /// Mock schema registry client. Implements <see cref="ISchemaRegistryClient"/>.
    /// </summary>
    public class MockSchemaRegistryClient : ISchemaRegistryClient
    {
        private class RegisterSchema
        {
            public string Schema { get; set; }
            public int Version { get; set; }
            public int Id { get; set; }
        }

        private int id = 0;
        private readonly List<string> subjects = new List<string>();
        private readonly Dictionary<string, List<RegisterSchema>> schemas = new Dictionary<string, List<RegisterSchema>>();

        #region ISchemaRegistryClient Impl

        /// <summary>
        /// The maximum capacity of the local schema cache.
        /// It's hardcoded to 100.
        /// </summary>
        public int MaxCachedSchemas { get; private set; } = 100;

        /// <summary>
        /// Request timeout
        /// </summary>
        public int RequestTimeoutMs { get; private set; }

        /// <summary>
        /// DEPRECATED. SubjectNameStrategy should now be specified via serializer configuration.
        /// Returns the schema registry key subject name given a topic name.
        /// </summary>
        /// <param name="topic">The topic name.</param>
        /// <param name="recordType">The fully qualified record type.</param>
        /// <returns>The key subject name given a topic name.</returns>
        [Obsolete("SubjectNameStrategy should now be specified via serializer configuration. This method will be removed in a future release.")]
        public string ConstructKeySubjectName(string topic, string recordType = null)
            => $"{topic}-key";

        /// <summary>
        /// DEPRECATED.SubjectNameStrategy should now be specified via serializer configuration.
        /// Returns the schema registry value subject name given a topic name.
        /// </summary>
        /// <param name="topic">The topic name.</param>
        /// <param name="recordType">The fully qualified record type.</param>
        /// <returns>The value subject name given a topic name.</returns>
        [Obsolete("SubjectNameStrategy should now be specified via serializer configuration. This method will be removed in a future release.")]
        public string ConstructValueSubjectName(string topic, string recordType = null)
            => $"{topic}-value";

        /// <summary>
        /// Disposable method.
        /// </summary>
        public void Dispose()
        {
            subjects.Clear();
            schemas.Clear();
            id = 0;
        }

        /// <summary>
        /// Gets a list of all subjects with registered schemas.
        /// </summary>
        /// <returns>A list of all subjects with registered schemas.</returns>
        public Task<List<string>> GetAllSubjectsAsync()
            => Task.FromResult(subjects);

        /// <summary>
        /// Set the current <seealso cref="SchemaRegistryConfig"/> instance.
        /// </summary>
        /// <param name="config">the current <seealso cref="SchemaRegistryConfig"/> instance</param>
        public void UseConfiguration(SchemaRegistryConfig config)
        {
            MaxCachedSchemas = config.MaxCachedSchemas.HasValue ? config.MaxCachedSchemas.Value : 100;
            RequestTimeoutMs = config.RequestTimeoutMs.HasValue ? config.RequestTimeoutMs.Value : 30000;
        }

        /// <summary>
        /// Get the latest schema registered against the specified subject.
        /// </summary>
        /// <param name="subject">The subject to get the latest associated schema for.</param>
        /// <returns>The latest schema registered against subject.</returns>
        public Task<RegisteredSchema> GetLatestSchemaAsync(string subject)
        {
            if (schemas.ContainsKey(subject))
            {
                var schema = schemas[subject];
                var s = schema.OrderByDescending(s => s.Version).FirstOrDefault();
                return s != null ? Task.FromResult(
                    new RegisteredSchema(
                        subject,
                        s.Version,
                        s.Id,
                        s.Schema,
                        SchemaType.Avro,
                        new List<SchemaReference>())
                    ) : Task.FromResult((RegisteredSchema)null);
            }
            else
            {
                return Task.FromResult((RegisteredSchema)null);
            }
        }
        

        /// <summary>
        /// Gets a schema given a subject and version number.
        /// </summary>
        /// <param name="subject">The subject to get the schema for.</param>
        /// <param name="version">The version number of schema to get.</param>
        /// <returns>The schema identified by the specified subject and version.</returns>
        public Task<RegisteredSchema> GetRegisteredSchemaAsync(string subject, int version)
        {
            if (schemas.ContainsKey(subject))
            {
                var schema = schemas[subject];
                var s = schema.FirstOrDefault(s => s.Version.Equals(version));
                return s != null ? Task.FromResult(
                    new RegisteredSchema(
                        subject,
                        s.Version,
                        s.Id,
                        s.Schema,
                        SchemaType.Avro,
                        new List<SchemaReference>())
                    ) : Task.FromResult((RegisteredSchema)null);
            }
            else
            {
                return Task.FromResult((RegisteredSchema)null);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="subject"></param>
        /// <param name="avroSchema"></param>
        /// <param name="normalize"></param>
        /// <returns></returns>
        public Task<int> RegisterSchemaAsync(string subject, string avroSchema, bool normalize = false)
            => RegisterSchemaAsync(subject, avroSchema);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="subject"></param>
        /// <param name="schema"></param>
        /// <param name="normalize"></param>
        /// <returns></returns>
        public Task<int> RegisterSchemaAsync(string subject, Schema schema, bool normalize = false)
            => RegisterSchemaAsync(subject, schema);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="subject"></param>
        /// <param name="avroSchema"></param>
        /// <param name="normalize"></param>
        /// <returns></returns>
        public Task<int> GetSchemaIdAsync(string subject, string avroSchema, bool normalize = false)
            => GetSchemaIdAsync(subject, avroSchema);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="subject"></param>
        /// <param name="schema"></param>
        /// <param name="normalize"></param>
        /// <returns></returns>
        public Task<int> GetSchemaIdAsync(string subject, Schema schema, bool normalize = false)
            => GetSchemaIdAsync(subject, schema);

        /// <summary>
        /// Gets the schema uniquely identified by id.
        /// </summary>
        /// <param name="id">The unique id of schema to get.</param>
        /// <param name="format">The format of the schema to get.</param>
        /// <returns>The schema identified by id.</returns>
        public Task<Schema> GetSchemaAsync(int id, string format = null)
        {
            var schema = schemas
                            .SelectMany(s => s.Value)
                            .FirstOrDefault(s => s.Id.Equals(id));
            return schema != null ?
                Task.FromResult(new Schema(schema.Schema, SchemaType.Avro)) :
                Task.FromResult((Schema)null);
        }

        /// <summary>
        /// DEPRECATED. Superseded by GetRegisteredSchemaAsync(string subject, int version)
        /// Gets a schema given a subject and version number.
        /// </summary>
        /// <param name="subject">The subject to get the schema for.</param>
        /// <param name="version">The version number of schema to get.</param>
        /// <returns>The schema identified by the specified subject and version.</returns>
        [Obsolete("Superseded by GetRegisteredSchemaAsync(string subject, int version). This method will be removed in a future release.")]
        public Task<string> GetSchemaAsync(string subject, int version)
        {
            if (schemas.ContainsKey(subject))
            {
                var schema = schemas[subject];
                var s = schema.FirstOrDefault(s => s.Version.Equals(version));
                return s != null ?
                    Task.FromResult(s.Schema) :
                    Task.FromResult((string)null);
            }
            else
            {
                return Task.FromResult((string)null);
            }
        }

        /// <summary>
        /// Get the unique id of the specified avro schema registered against the specified subject.
        /// </summary>
        /// <param name="subject">The subject the schema is registered against.</param>
        /// <param name="avroSchema">The schema to get the id for.</param>
        /// <returns>The unique id identifying the schema.</returns>
        [Obsolete("Superseded by GetSchemaIdAsync(string, Schema)")]
        public Task<int> GetSchemaIdAsync(string subject, string avroSchema)
        {
            if (schemas.ContainsKey(subject))
            {
                var schema = schemas[subject];
                var s = schema.FirstOrDefault(s => s.Schema.Equals(avroSchema));
                return s != null ?
                    Task.FromResult(s.Id) :
                    Task.FromResult(-1);
            }
            else
            {
                return Task.FromResult(-1);
            }
        }

        /// <summary>
        /// Get the unique id of the specified schema registered against the specified subject.
        /// </summary>
        /// <param name="subject">The subject the schema is registered against.</param>
        /// <param name="schema">The schema to get the id for.</param>
        /// <returns>The unique id identifying the schema.</returns>
        public Task<int> GetSchemaIdAsync(string subject, Schema schema)
            => GetSchemaIdAsync(subject, schema.SchemaString);

        /// <summary>
        /// Gets a list of versions registered under the specified subject.
        /// </summary>
        /// <param name="subject">The subject to get versions registered under.</param>
        /// <returns>A list of versions registered under the specified subject.</returns>
        public Task<List<int>> GetSubjectVersionsAsync(string subject)
        {
            if (schemas.ContainsKey(subject))
            {
                var schema = schemas[subject];
                return Task.FromResult(schema.Select(s => s.Id).ToList());
            }
            else
            {
                return Task.FromResult(new List<int>());
            }
        }

        /// <summary>
        /// Check if an avro schema is compatible with latest version registered against
        /// a specified subject.
        /// </summary>
        /// <param name="subject">The subject to check.</param>
        /// <param name="avroSchema">The schema to check.</param>
        /// <returns>true if avroSchema is compatible with the latest version registered against a specified subject, false otherwise.</returns>
        [Obsolete("Superseded by IsCompatibleAsync(string, Schema)")]
        public Task<bool> IsCompatibleAsync(string subject, string avroSchema)
            => Task.FromResult(true);

        /// <summary>
        /// Check if an avro schema is compatible with latest version registered against
        /// a specified subject.
        /// </summary>
        /// <param name="subject">The subject to check.</param>
        /// <param name="schema">The schema to check.</param>
        /// <returns>true if avroSchema is compatible with the latest version registered against a specified subject, false otherwise.</returns>
        public Task<bool> IsCompatibleAsync(string subject, Schema schema)
            => IsCompatibleAsync(subject, schema.SchemaString);

        /// <summary>
        /// Returns the latest schema registered against the specified subject.
        /// </summary>
        /// <param name="subject"></param>
        /// <param name="schema"></param>
        /// <param name="ignoreDeletedSchemas"></param>
        /// <returns></returns>
        public Task<RegisteredSchema> LookupSchemaAsync(string subject, Schema schema, bool ignoreDeletedSchemas)
        {
            return GetLatestSchemaAsync(subject);
        }

        public Task<RegisteredSchema> LookupSchemaAsync(string subject, Schema schema, bool ignoreDeletedSchemas,
            bool normalize = false)
            => LookupSchemaAsync(subject, schema, ignoreDeletedSchemas);

        /// <summary>
        /// Register an Avro schema or get the schema id if it's already registered.
        /// </summary>
        /// <param name="subject">The subject to register the schema against.</param>
        /// <param name="avroSchema">The schema to register.</param>
        /// <returns>A unique id identifying the schema.</returns>
        [Obsolete("Superseded by RegisterSchemaAsync(string, Schema)")]
        public Task<int> RegisterSchemaAsync(string subject, string avroSchema)
        {
            ++id;
            if (schemas.ContainsKey(subject))
            {
                var maxVersion = schemas[subject].Max(s => s.Version) + 1;
                schemas[subject].Add(new RegisterSchema
                {
                    Id = id,
                    Schema = avroSchema,
                    Version = maxVersion
                });
            }
            else
            {
                schemas.Add(subject, new List<RegisterSchema> {
                    new RegisterSchema {
                    Id = id,
                    Schema = avroSchema,
                    Version = 1
                    }
                });
                subjects.Add(subject);
            }
            return Task.FromResult(id);
        }

        /// <summary>
        /// Register an Avro schema or get the schema id if it's already registered.
        /// </summary>
        /// <param name="subject">The subject to register the schema against.</param>
        /// <param name="schema">The schema to register.</param>
        /// <returns>A unique id identifying the schema.</returns>
        public Task<int> RegisterSchemaAsync(string subject, Schema schema)
            => RegisterSchemaAsync(subject, schema.SchemaString);
        
        /// <summary>
        ///     If the subject is specified returns compatibility type for the specified subject.
        ///     Otherwise returns global compatibility type.
        /// </summary>
        /// <param name="subject">
        ///     The subject to get the compatibility for.
        /// </param>
        /// <returns>Compatibility type.</returns>
        public Task<Compatibility> GetCompatibilityAsync(string subject = null)
        {
            return Task.FromResult(Compatibility.None);
        }
        
        /// <summary>
        ///     If the subject is specified sets compatibility type for the specified subject.
        ///     Otherwise sets global compatibility type.
        /// </summary>
        /// <param name="subject">
        ///      The subject to set the compatibility for.
        /// </param>
        /// <param name="compatibility">Compatibility type.</param>
        /// <returns>New compatibility type.</returns>
        public Task<Compatibility> UpdateCompatibilityAsync(Compatibility compatibility, string subject = null)
        {
            return Task.FromResult(compatibility);
        }

        #endregion ISchemaRegistryClient Impl
    }
}