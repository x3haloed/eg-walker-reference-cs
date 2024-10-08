namespace EgWalkerReference
{
    public struct RawVersion : IComparable<RawVersion>
    {
        public string Agent;
        public int Seq;

        public RawVersion(string agent, int seq)
        {
            Agent = agent;
            Seq = seq;
        }

        public int CompareTo(RawVersion other)
        {
            int agentComparison = string.Compare(Agent, other.Agent, StringComparison.Ordinal);
            if (agentComparison != 0)
                return agentComparison;
            return Seq.CompareTo(other.Seq);
        }
    }
}