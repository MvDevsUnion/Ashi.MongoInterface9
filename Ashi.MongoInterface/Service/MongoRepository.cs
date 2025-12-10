using Ashi.MongoInterface.Helper;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Linq.Expressions;

namespace Ashi.MongoInterface.Service
{
    #region Irepo

    public interface IMongoRepository<TDocument> where TDocument : IDocument
    {
        IQueryable<TDocument> AsQueryable();
        MongoDB.Bson.Serialization.IBsonSerializerRegistry Settings_SerializerRegistry();
        IEnumerable<TDocument> Aggregate<TDocument>(BsonDocument[] pipeline);
        MongoDB.Bson.Serialization.IBsonSerializer<TDocument> DocumentSerializer();
        IEnumerable<TDocument> FilterBy(
            Expression<Func<TDocument, bool>> filterExpression);

        IEnumerable<TProjected> FilterBy<TProjected>(
            Expression<Func<TDocument, bool>> filterExpression,
            Expression<Func<TDocument, TProjected>> projectionExpression);

        TDocument FindOne(Expression<Func<TDocument, bool>> filterExpression);
        IEnumerable<TDocument> Find(FilterDefinition<TDocument> filter, int amount);

        Task<long> CountDocumentsAsync(FilterDefinition<TDocument> filter);

        /// <summary>
        /// pull the entier collection
        /// </summary>
        /// <param name="filter"></param>
        /// <returns></returns>
        IEnumerable<TDocument> Find(FilterDefinition<TDocument> filter);

        Task<(int totalPages, IReadOnlyList<TDocument> data, long? totalCount)> AggregateByPages
                    (
                    FilterDefinition<TDocument> filterDefinition,
                    SortDefinition<TDocument> sortDefinition,
                    int page,
                    int pageSize
                    );

        IEnumerable<TDocument> Find(FilterDefinition<TDocument> filter, SortDefinition<TDocument> sort, int limit);

        Task<TDocument> FindOneAsync(Expression<Func<TDocument, bool>> filterExpression);

        TDocument FindById(Guid id);

        Task<TDocument> FindByIdAsync(Guid id);

        void InsertOne(TDocument document);

        Task InsertOneAsync(TDocument document);

        void InsertMany(ICollection<TDocument> documents);

        Task InsertManyAsync(ICollection<TDocument> documents);

        void ReplaceOne(TDocument document);

        Task ReplaceOneAsync(TDocument document);

        void DeleteOne(Expression<Func<TDocument, bool>> filterExpression);

        Task DeleteOneAsync(Expression<Func<TDocument, bool>> filterExpression);

        void DeleteById(Guid id);

        Task DeleteByIdAsync(Guid id);

        void DeleteMany(Expression<Func<TDocument, bool>> filterExpression);

        Task DeleteManyAsync(Expression<Func<TDocument, bool>> filterExpression);
        Task DeleteManyAsync(FilterDefinition<TDocument> filter);

        Task UpdateOneAsync(FilterDefinition<TDocument> filter, UpdateDefinition<TDocument> update);
        Task UpdateManyAsync(FilterDefinition<TDocument> filter, UpdateDefinition<TDocument> update);
        Task CreateManyIndexAsync(List<CreateIndexModel<TDocument>> indexList);
        Task CreateIndexAsync(CreateIndexModel<TDocument> index);
    }

    #endregion


    /// <summary>
    /// read the artical for documentation
    /// https://medium.com/@marekzyla95/mongo-repository-pattern-700986454a0e
    /// </summary>
    /// <typeparam name="TDocument"></typeparam>
    public class MongoRepository<TDocument> : IMongoRepository<TDocument>
    where TDocument : IDocument
    {
        private readonly IMongoCollection<TDocument> _collection;

        public MongoRepository(IMongoDbSettings settings)
        {
            MongoClientSettings mongoClientSettings = MongoClientSettings.FromConnectionString(settings.ConnectionString);

            mongoClientSettings.MaxConnectionPoolSize = 1000;


            var database = new MongoClient(mongoClientSettings).GetDatabase(settings.DatabaseName);
            _collection = database.GetCollection<TDocument>(GetCollectionName(typeof(TDocument)));
        }

        private protected string GetCollectionName(Type documentType)
        {
            return ((BsonCollectionAttribute)documentType.GetCustomAttributes(
                    typeof(BsonCollectionAttribute),
                    true)
                .FirstOrDefault())?.CollectionName;
        }


        public virtual IEnumerable<TDocument> Aggregate<TDocument>(BsonDocument[] pipeline)
        {
            return _collection.Aggregate<TDocument>(pipeline).ToEnumerable();
        }

        public virtual MongoDB.Bson.Serialization.IBsonSerializer<TDocument> DocumentSerializer()
        {
            return _collection.DocumentSerializer;
        }

        public virtual MongoDB.Bson.Serialization.IBsonSerializerRegistry Settings_SerializerRegistry()
        {
            return _collection.Settings.SerializerRegistry;
        }

        public virtual IQueryable<TDocument> AsQueryable()
        {
            return _collection.AsQueryable();
        }

        public virtual IEnumerable<TDocument> FilterBy(
            Expression<Func<TDocument, bool>> filterExpression)
        {
            return _collection.Find(filterExpression).ToEnumerable();
        }

        public virtual IEnumerable<TProjected> FilterBy<TProjected>(Expression<Func<TDocument, bool>> filterExpression, Expression<Func<TDocument, TProjected>> projectionExpression)
        {
            return _collection.Find(filterExpression).Project(projectionExpression).ToEnumerable();
        }

        public virtual TDocument FindOne(Expression<Func<TDocument, bool>> filterExpression)
        {
            return _collection.Find(filterExpression).FirstOrDefault();
        }

        public virtual IEnumerable<TDocument> Find(FilterDefinition<TDocument> filter, int amount)
        {
            return _collection.Find(filter).Limit(amount).ToEnumerable();
        }

        public virtual IEnumerable<TDocument> Find(FilterDefinition<TDocument> filter)
        {
            return _collection.Find(filter).ToEnumerable();
        }



        public virtual IEnumerable<TDocument> Find(FilterDefinition<TDocument> filter, SortDefinition<TDocument> sort, int limit)
        {
            return _collection.Find(filter).Sort(sort).Limit(limit).ToEnumerable();
        }

        public virtual Task<TDocument> FindOneAsync(Expression<Func<TDocument, bool>> filterExpression)
        {
            return Task.Run(() => _collection.Find(filterExpression).FirstOrDefaultAsync());
        }

        public virtual TDocument FindById(Guid id)
        {
            var filter = Builders<TDocument>.Filter.Eq(doc => doc.Id, id);
            return _collection.Find(filter).SingleOrDefault();
        }

        public virtual Task<TDocument> FindByIdAsync(Guid id)
        {
            return Task.Run(() =>
            {
                var filter = Builders<TDocument>.Filter.Eq(doc => doc.Id, id);
                return _collection.Find(filter).SingleOrDefaultAsync();
            });
        }


        public virtual void InsertOne(TDocument document)
        {
            _collection.InsertOne(document);
        }

        public virtual Task InsertOneAsync(TDocument document)
        {
            return Task.Run(() => _collection.InsertOneAsync(document));
        }

        public void InsertMany(ICollection<TDocument> documents)
        {
            _collection.InsertMany(documents);
        }


        public virtual async Task InsertManyAsync(ICollection<TDocument> documents)
        {
            await _collection.InsertManyAsync(documents);
        }

        public void ReplaceOne(TDocument document)
        {
            var filter = Builders<TDocument>.Filter.Eq(doc => doc.Id, document.Id);
            _collection.FindOneAndReplace(filter, document);
        }

        public virtual async Task ReplaceOneAsync(TDocument document)
        {
            var filter = Builders<TDocument>.Filter.Eq(doc => doc.Id, document.Id);
            await _collection.FindOneAndReplaceAsync(filter, document);
        }

        public void DeleteOne(Expression<Func<TDocument, bool>> filterExpression)
        {
            _collection.FindOneAndDelete(filterExpression);
        }

        public Task DeleteOneAsync(Expression<Func<TDocument, bool>> filterExpression)
        {
            return Task.Run(() => _collection.FindOneAndDeleteAsync(filterExpression));
        }

        public void DeleteById(Guid id)
        {
            var filter = Builders<TDocument>.Filter.Eq(doc => doc.Id, id);
            _collection.FindOneAndDelete(filter);
        }

        public Task DeleteByIdAsync(Guid id)
        {
            return Task.Run(() =>
            {
                var filter = Builders<TDocument>.Filter.Eq(doc => doc.Id, id);
                _collection.FindOneAndDeleteAsync(filter);
            });
        }

        public void DeleteMany(Expression<Func<TDocument, bool>> filterExpression)
        {
            _collection.DeleteMany(filterExpression);
        }

        public Task DeleteManyAsync(Expression<Func<TDocument, bool>> filterExpression)
        {
            return Task.Run(() => _collection.DeleteManyAsync(filterExpression));
        }

        public Task DeleteManyAsync(FilterDefinition<TDocument> filter)
        {
            return Task.Run(() => _collection.DeleteManyAsync(filter));
        }

        public Task<long> CountDocumentsAsync(FilterDefinition<TDocument> filter)
        {
            return Task.Run(() => _collection.CountDocumentsAsync(filter));
        }

        public async Task UpdateOneAsync(FilterDefinition<TDocument> filter, UpdateDefinition<TDocument> update)
        {
            var result = await _collection.UpdateOneAsync(filter, update);

        }

        public async Task UpdateManyAsync(FilterDefinition<TDocument> filter, UpdateDefinition<TDocument> update)
        {
            var result = await _collection.UpdateManyAsync(filter, update);

        }

        public async Task<IEnumerable<TDocument>> GetDocumentAsync(string field_name)
        {
            var categoryTasks = _collection.Find<TDocument>(x => x.Id != null)
                     .Project(Builders<TDocument>.Projection
                                                .Include("field_name")
                                                .Exclude("_id")).ToEnumerable();


            return (IEnumerable<TDocument>)categoryTasks;
        }

        public async Task<(int totalPages, IReadOnlyList<TDocument> data, long? totalCount)> AggregateByPages(FilterDefinition<TDocument> filterDefinition, SortDefinition<TDocument> sortDefinition, int page, int pageSize)
        {
            var countFacet = AggregateFacet.Create("count",
                PipelineDefinition<TDocument, AggregateCountResult>.Create(new[]
                {
                PipelineStageDefinitionBuilder.Count<TDocument>()
                }));

            var dataFacet = AggregateFacet.Create("data",
                PipelineDefinition<TDocument, TDocument>.Create(new[]
                {
                PipelineStageDefinitionBuilder.Sort(sortDefinition),
                PipelineStageDefinitionBuilder.Skip<TDocument>((page - 1) * pageSize),
                PipelineStageDefinitionBuilder.Limit<TDocument>(pageSize),
                }));


            var aggregation = await _collection.Aggregate()
                .Match(filterDefinition)
                .Facet(countFacet, dataFacet)
                .ToListAsync();

            var count = aggregation.First()
                .Facets.First(x => x.Name == "count")
                .Output<AggregateCountResult>()
                ?.FirstOrDefault()
                ?.Count;

            if (count == null)
                count = 1;

            var totalPages = (int)Math.Ceiling((double)count / pageSize);

            var data = aggregation.First()
                .Facets.First(x => x.Name == "data")
                .Output<TDocument>();

            return (totalPages, data, count);
        }

        public async Task CreateManyIndexAsync(List<CreateIndexModel<TDocument>> indexList)
        {
            await _collection.Indexes.CreateManyAsync(indexList);
        }

        public async Task CreateIndexAsync(CreateIndexModel<TDocument> index)
        {
            await _collection.Indexes.CreateOneAsync(index);
        }


        //public async Task UpdateLastActiveActivity(Guid id)
        //{
        //    var updateDef = Builders<User>.Update.Set(o => o.Id, id);
        //    await _collection.UpdateOneAsync(o => o.LastActive == DateTime.Now, updateDef);
        //}
    }
}
