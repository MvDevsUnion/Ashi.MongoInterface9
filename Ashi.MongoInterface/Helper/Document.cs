using MongoDB.Bson;
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
        [BsonId]
        [BsonGuidRepresentation(GuidRepresentation.Standard)]
        public Guid Id { get; set; }
    }

}
