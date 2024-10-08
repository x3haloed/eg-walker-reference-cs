namespace EgWalkerReference
{
    public class ListOp<T>
    {
        public string Type { get; set; } // Either "ins" or "del"
        public int Pos { get; set; }
        public T Content { get; set; } // Nullable for delete operations

        public ListOp(string type, int pos, T content = default(T))
        {
            Type = type;
            Pos = pos;
            Content = content;
        }
    }
}