using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace TwitterMining
{
    class Program
    {
        static HashSet<Node> nodes;
        static HashSet<Edge> edges;
        static void Main(string[] args)
        {
            nodes = new(ExtractNodes(save: true));
            edges = new(ExtractEdges(save: true));
            var trimmedNodes = TrimNodes(save: true);
            Debugger.Break();
 
            //JArray tweets = JsonConvert.DeserializeObject<JArray>(File.ReadAllText("data/cc_tweets.json"));
            //var dictionary = tweets.GroupBy((x) => x["lang"].ToString()).ToDictionary((x) => x.Key, (x) => x.ToArray());
            //foreach (var item in dictionary)
            //{
            //    Debug.Prlong($"{item.Key}: {item.Value.Length}");
            //}
        }

        private static IEnumerable<Node> ExtractNodes(bool save = false)
        {
            JArray users = JsonConvert.DeserializeObject<JArray>(File.ReadAllText("data/cc_users.json"))!;
            var relevantUsers = users.Where((x) => x["seed"]?.ToString() == "1148654208398319622");
            IEnumerable<Node> nodes = relevantUsers.Select((x) => new Node(
                long.Parse(x["id"]!.ToString()),
                x["name"]!.ToString(),
                x["location"]?.ToString()
            ));
            if (save)
                File.WriteAllText("nodes.json", JsonConvert.SerializeObject(nodes));
            return nodes;
        }

        private static Node? GetNodeById(long id)
            => nodes.Where((x) => x.id == id).FirstOrDefault();

        private static ConcurrentBag<Edge> ExtractEdges(bool save = false)
        {
            using (var stream = File.OpenRead("data/cc_follows.json"))
            using (StreamReader streamReader = new StreamReader(stream))
            using (JsonReader reader = new JsonTextReader(streamReader))
            {
                JsonSerializer serializer = new JsonSerializer();
                ConcurrentBag<Edge> edges = new();
                var follows = serializer.Deserialize<FollowData[]>(reader)!; // JsonConvert.DeserializeObject<JArray>(stream)!;
                Parallel.ForEach(follows, (x) =>
                {
                    Node? currentNode = GetNodeById(x.id);
                    if (currentNode != null)
                    {
                        var following = x.following;
                        Parallel.ForEach(following, (id) =>
                        {
                            Node? connectedNode = GetNodeById(long.Parse(id.ToString()));
                            if (connectedNode != null)
                                edges.Add(new(currentNode.id, connectedNode.id));
                        });
                        var followed_by = x.followed_by;
                        Parallel.ForEach(followed_by, (id) =>
                        {
                            Node? connectedNode = GetNodeById(long.Parse(id.ToString()));
                            if (connectedNode != null)
                                edges.Add(new(currentNode.id, connectedNode.id));
                        });
                    }
                });
                if (save)
                    File.WriteAllText("edges.json", JsonConvert.SerializeObject(edges));
                return edges;
            }
        }

        private static HashSet<Node> TrimNodes(bool save = false)
        {
            HashSet<Node> nodesWithEdge = new();
            HashSet<long> idsWithEdge = new();
            foreach(var edge in edges)
            {
                if (!idsWithEdge.Contains(edge.startId))
                    idsWithEdge.Add(edge.startId);
                if (!idsWithEdge.Contains(edge.endId))
                    idsWithEdge.Add(edge.endId);
            }
            foreach(var node in nodes)
            {
                if (idsWithEdge.Contains(node.id))
                    nodesWithEdge.Add(node);
            }
            if (save)
                File.WriteAllText("nodes_with_edge.json", JsonConvert.SerializeObject(nodesWithEdge));
            return nodesWithEdge;
        }

        record class Node(long id, string name, string? ort);
        record class Edge(long startId, long endId);

        record class FollowData(long id, long[] following, long[] followed_by);
    }
}
