namespace EgWalkerReference
{
    public class Branch<T>
    {
        public List<T> Snapshot { get; set; }
        public List<int> Version { get; set; }

        public Branch()
        {
            Snapshot = new List<T>();
            Version = new List<int>();
        }
    }
}