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
            nodes = new(ExtractNodes());
            edges = new(ExtractEdges());
            {
                TsvSerializer<Edge> serializer = new();
                using (var stream = File.Create("edges.csv"))
                    serializer.Serialize(edges, stream);
            }
            var trimmedNodes = TrimNodes();
            {
                TsvSerializer<Node> serializer = new();
                using (var stream = File.Create("nodes.csv"))
                    serializer.Serialize(trimmedNodes, stream);
            }
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
                x["location"]?.ToString(),
                x["created_at"]!.ToString(),
                (bool)x["verified"]!
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
            using (StreamReader streamReader = new(stream))
            using (JsonTextReader reader = new(streamReader))
            {
                JsonSerializer serializer = new JsonSerializer();
                ConcurrentBag<Edge> edges = new();
                var follows = serializer.Deserialize<FollowData[]>(reader)!;
                Parallel.ForEach(follows, (x) =>
                {
                    Node? currentNode = GetNodeById(x.id);
                    if (currentNode != default)
                    {
                        var following = x.following;
                        // Parallel.ForEach(following, (id) =>
                        foreach (var id in following)
                        {
                            Node? connectedNode = GetNodeById(long.Parse(id.ToString()));
                            if (connectedNode != null)
                                edges.Add(new(currentNode.id, connectedNode.id));
                        };
                        var followed_by = x.followed_by;
                        // Parallel.ForEach(followed_by, (id) =>
                        foreach (var id in followed_by)
                        {
                            Node? connectedNode = GetNodeById(long.Parse(id.ToString()));
                            if (connectedNode != null)
                                edges.Add(new(currentNode.id, connectedNode.id));
                        };
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
            foreach (var edge in edges)
            {
                if (!idsWithEdge.Contains(edge.startId))
                    idsWithEdge.Add(edge.startId);
            }
            foreach (var node in nodes)
            {
                if (idsWithEdge.Contains(node.id))
                    nodesWithEdge.Add(node);
            }
            if (save)
                File.WriteAllText("nodes_with_edge.json", JsonConvert.SerializeObject(nodesWithEdge));
            return nodesWithEdge;
        }

        record class Node(long id, string name, string? ort, string created_at, bool verified);
        record class Edge(long startId, long endId);

        record class FollowData(long id, long[] following, long[] followed_by);
    }
}
