namespace Ashi.MongoInterface.Helper
{
    [AttributeUsage(AttributeTargets.Class, Inherited = false)]
    public class BsonCollectionAttribute : Attribute
    {
        public string CollectionName { get; }

        public BsonCollectionAttribute(string collectionName)
        {
            CollectionName = collectionName;
        }
    }


    [AttributeUsage(AttributeTargets.Property)]
    public class ObsoletePropertyUsageAttribute : Attribute
    {
        public ObsoletePropertyUsageAttribute(string message)
        {
            Message = message;
        }

        public string Message { get; }
    }
}
