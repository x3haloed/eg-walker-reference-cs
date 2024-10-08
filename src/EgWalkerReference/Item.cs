namespace EgWalkerReference
{
    public class Item
    {
        public ItemState CurState { get; set; }
        public ItemState EndState { get; set; }
        public int OpId { get; set; }
        public int OriginLeft { get; set; }
        public int RightParent { get; set; }
    }
}