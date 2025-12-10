using MongoDB.Bson.Serialization.Attributes;

namespace Ashi.MongoInterface.Helper
{
    public interface IDocument
    {
        //[JsonConverter(typeof(ObjectIdConverter))]
        [BsonId]
        Guid Id { get; set; }
    }

    public abstract class Document : IDocument
    {
        public Guid Id { get; set; }
    }
}
