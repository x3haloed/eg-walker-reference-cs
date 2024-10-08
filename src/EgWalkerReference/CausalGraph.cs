namespace EgWalkerReference
{
    public class CausalGraph
    {
        public List<int> Heads;
        public List<CGEntry> Entries;
        public Dictionary<string, List<ClientEntry>> AgentToVersion;

        public CausalGraph()
        {
            Heads = new List<int>();
            Entries = new List<CGEntry>();
            AgentToVersion = new Dictionary<string, List<ClientEntry>>();
        }
    }
}