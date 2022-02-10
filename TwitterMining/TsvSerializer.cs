using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace TwitterMining
{
    public sealed class TsvSerializer<T>
    {
        private const string Seperator = "\t";
        public void Serialize(IEnumerable<T> data, Stream stream)
        {
            using (StreamWriter writer = new(stream))
            {
                var properties = typeof(T).GetProperties();
                writer.WriteLine(string.Join(Seperator, properties.Select((prop) => prop.Name)));
                foreach (var item in data)
                    writer.WriteLine(string.Join(Seperator, properties.Select((prop) =>
                    {
                        object? value = prop.GetValue(item);
                        if (value == null)
                            return "NaN";
                        else
                            return value.ToString();
                    })));
                writer.Flush();
            }
        }
    }
}
