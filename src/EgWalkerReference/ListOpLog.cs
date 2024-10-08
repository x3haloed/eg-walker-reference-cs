namespace EgWalkerReference
{
    public class ListOpLog<T>
    {
        public List<ListOp<T>> Ops { get; set; }
        public CausalGraph Cg { get; set; }

        public ListOpLog()
        {
            Ops = new List<ListOp<T>>();
            Cg = new CausalGraph();
        }
    }
}