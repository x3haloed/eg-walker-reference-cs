namespace EgWalkerReference
{
    public class EditContext<T>
    {
        public List<Item> Items { get; set; }
        public List<int> DelTargets { get; set; }
        public List<Item> ItemsByLV { get; set; }
        public List<int> CurVersion { get; set; }
    }
}