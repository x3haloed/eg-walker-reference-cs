namespace EgWalkerReference
{
    public class CGEntry
    {
        public int Version;
        public int VEnd;
        public string Agent;
        public int Seq;
        public List<int> Parents;

        public CGEntry()
        {
            Parents = new List<int>();
        }
    }
}