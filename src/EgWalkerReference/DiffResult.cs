namespace EgWalkerReference
{
    public class DiffResult
    {
        public List<LVRange> AOnly { get; set; }
        public List<LVRange> BOnly { get; set; }

        public DiffResult()
        {
            AOnly = new List<LVRange>();
            BOnly = new List<LVRange>();
        }
    }
}