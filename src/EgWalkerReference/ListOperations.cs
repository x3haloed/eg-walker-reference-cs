namespace EgWalkerReference
{
    public static class ListOperations
    {
        public static ListOpLog<T> CreateOpLog<T>()
        {
            return new ListOpLog<T>();
        }

        public static void LocalInsert<T>(ListOpLog<T> oplog, string agent, int pos, params T[] content)
        {
            int seq = Utils.NextSeqForAgent(oplog.Cg, agent);
            Utils.Add(oplog.Cg, agent, seq, seq + content.Length, oplog.Cg.Heads);

            foreach (var val in content)
            {
                oplog.Ops.Add(new ListOp<T>("ins", pos, val));
                pos++;
            }
        }

        public static void LocalDelete<T>(ListOpLog<T> oplog, string agent, int pos, int len = 1)
        {
            if (len == 0) throw new ArgumentException("Invalid delete length");

            int seq = Utils.NextSeqForAgent(oplog.Cg, agent);
            Utils.Add(oplog.Cg, agent, seq, seq + len, oplog.Cg.Heads);

            for (int i = 0; i < len; i++)
            {
                oplog.Ops.Add(new ListOp<T>("del", pos));
            }
        }

        public static bool PushOp<T>(ListOpLog<T> oplog, RawVersion id, List<RawVersion> parents, string type, int pos, T content = default(T))
        {
            var entry = Utils.AddRaw(oplog.Cg, id, 1, parents);
            if (entry == null) return false;

            if (type == "ins" && EqualityComparer<T>.Default.Equals(content, default(T)))
                throw new ArgumentException("Cannot add an insert operation with no content");

            var op = new ListOp<T>(type, pos, content);
            oplog.Ops.Add(op);
            return true;
        }

        public static List<RawVersion> GetLatestVersion<T>(ListOpLog<T> oplog)
        {
            return Utils.LvToRawList(oplog.Cg, oplog.Cg.Heads);
        }

        public static void MergeOplogInto<T>(ListOpLog<T> dest, ListOpLog<T> src)
        {
            var vs = Utils.SummarizeVersion(dest.Cg);
            var (commonVersion, _) = Utils.IntersectWithSummary(src.Cg, vs);
            var ranges = Utils.Diff(src.Cg, commonVersion, src.Cg.Heads).BOnly;

            if (ranges.Count == 0)
            {
                // No new versions to merge
                return;
            }

            var cgDiff = Utils.SerializeDiff(src.Cg, ranges);
            Utils.MergePartialVersions(dest.Cg, cgDiff);

            foreach (var range in ranges)
            {
                for (int i = range.Start; i < range.End; i++)
                {
                    dest.Ops.Add(src.Ops[i]);
                }
            }
        }

        public static Branch<T> Checkout<T>(ListOpLog<T> oplog)
        {
            var ctx = new EditContext<T>
            {
                Items = new List<Item>(),
                DelTargets = Enumerable.Repeat(-1, oplog.Ops.Count).ToList(),
                ItemsByLV = Enumerable.Repeat<Item>(null, oplog.Ops.Count).ToList(),
                CurVersion = new List<int>()
            };

            var snapshot = new List<T>();
            TraverseAndApply(ctx, oplog, snapshot);

            return new Branch<T>
            {
                Snapshot = snapshot,
                Version = new List<int>(oplog.Cg.Heads)
            };
        }

        public static List<T> CheckoutSimple<T>(ListOpLog<T> oplog)
        {
            return Checkout(oplog).Snapshot;
        }

        public static string CheckoutSimpleString(ListOpLog<string> oplog)
        {
            return string.Join("", CheckoutSimple(oplog));
        }

        public static void TraverseAndApply<T>(EditContext<T> ctx, ListOpLog<T> oplog, List<T> snapshot, int fromOp = 0, int toOp = -1)
        {
            if (toOp == -1) toOp = Utils.NextLV(oplog.Cg);

            foreach (var entry in Utils.IterVersionsBetween(oplog.Cg, fromOp, toOp))
            {
                var diffResult = Utils.Diff(oplog.Cg, ctx.CurVersion, entry.Parents);
                foreach (var range in diffResult.AOnly.AsEnumerable().Reverse())
                {
                    for (int lv = range.End - 1; lv >= range.Start; lv--)
                    {
                        Retreat1(ctx, oplog, lv);
                    }
                }

                foreach (var range in diffResult.BOnly)
                {
                    for (int lv = range.Start; lv < range.End; lv++)
                    {
                        Advance1(ctx, oplog, lv);
                    }
                }

                for (int lv = entry.Version; lv < entry.VEnd; lv++)
                {
                    Apply1(ctx, snapshot, oplog, lv);
                }

                ctx.CurVersion = new List<int> { entry.VEnd - 1 };
            }
        }

        private static void Advance1<T>(EditContext<T> ctx, ListOpLog<T> oplog, int opId)
        {
            var op = oplog.Ops[opId];
            int targetLV = op.Type == "del" ? ctx.DelTargets[opId] : opId;
            var item = ctx.ItemsByLV[targetLV];

            if (op.Type == "del")
            {
                Utils.Assert(item.CurState >= ItemState.Inserted, "Invalid state - advance delete but item is not inserted");
                Utils.Assert(item.EndState >= ItemState.Deleted, "Advance delete with item not deleted in endState");
                item.CurState++;
            }
            else
            {
                Utils.AssertEq(item.CurState, ItemState.NotYetInserted, "Advance insert for already inserted item " + opId);
                item.CurState = ItemState.Inserted;
            }
        }

        private static void Retreat1<T>(EditContext<T> ctx, ListOpLog<T> oplog, int opId)
        {
            var op = oplog.Ops[opId];
            int targetLV = op.Type == "del" ? ctx.DelTargets[opId] : opId;
            var item = ctx.ItemsByLV[targetLV];

            if (op.Type == "del")
            {
                Utils.Assert(item.CurState >= ItemState.Deleted, "Retreat delete but item not currently deleted");
                Utils.Assert(item.EndState >= ItemState.Deleted, "Retreat delete but item not deleted");
                item.CurState--;
            }
            else
            {
                Utils.AssertEq(item.CurState, ItemState.Inserted, "Retreat insert for item not in inserted state");
                item.CurState = ItemState.NotYetInserted;
            }
        }

        private static void Apply1<T>(EditContext<T> ctx, List<T> snapshot, ListOpLog<T> oplog, int opId)
        {
            var op = oplog.Ops[opId];

            if (op.Type == "del")
            {
                var cursor = FindByCurPos(ctx, op.Pos);
                while (ctx.Items[cursor.Idx].CurState != ItemState.Inserted)
                {
                    var item = ctx.Items[cursor.Idx];
                    cursor.EndPos += Utils.ItemWidth(item.EndState);
                    cursor.Idx++;
                }

                var targetItem = ctx.Items[cursor.Idx];
                Utils.Assert(targetItem.CurState == ItemState.Inserted, "Trying to delete an item which is not currently inserted");

                if (targetItem.EndState == ItemState.Inserted && snapshot != null)
                {
                    snapshot.RemoveAt(cursor.EndPos);
                }

                targetItem.CurState = targetItem.EndState = ItemState.Deleted;
                ctx.DelTargets[opId] = targetItem.OpId;
            }
            else
            {
                var cursor = FindByCurPos(ctx, op.Pos);
                var originLeft = cursor.Idx == 0 ? -1 : ctx.Items[cursor.Idx - 1].OpId;
                int rightParent = -1;

                for (int i = cursor.Idx; i < ctx.Items.Count; i++)
                {
                    var nextItem = ctx.Items[i];
                    if (nextItem.CurState != ItemState.NotYetInserted)
                    {
                        rightParent = (nextItem.OriginLeft == originLeft) ? nextItem.OpId : -1;
                        break;
                    }
                }

                var newItem = new Item
                {
                    CurState = ItemState.Inserted,
                    EndState = ItemState.Inserted,
                    OpId = opId,
                    OriginLeft = originLeft,
                    RightParent = rightParent
                };

                ctx.ItemsByLV[opId] = newItem;
                Integrate(ctx, oplog.Cg, newItem, ref cursor);
                ctx.Items.Insert(cursor.Idx, newItem);

                if (snapshot != null)
                {
                    snapshot.Insert(cursor.EndPos, op.Content);
                }
            }
        }

        private static void Integrate<T>(EditContext<T> ctx, CausalGraph cg, Item newItem, ref DocCursor cursor)
        {
            // If there's no concurrency, we don't need to scan.
            if (cursor.Idx >= ctx.Items.Count || ctx.Items[cursor.Idx].CurState != ItemState.NotYetInserted) return;

            // Sometimes we need to scan ahead and maybe insert there, or maybe insert here.
            bool scanning = false;
            int scanIdx = cursor.Idx;
            int scanEndPos = cursor.EndPos;

            int leftIdx = cursor.Idx - 1;
            int rightIdx = newItem.RightParent == -1 ? ctx.Items.Count : Utils.FindItemIdx(ctx, newItem.RightParent);

            while (scanIdx < ctx.Items.Count)
            {
                var other = ctx.Items[scanIdx];

                // When concurrent inserts happen, the newly inserted item goes somewhere between the
                // insert position itself (passed in through cursor) to the next item that existed
                // when which the insert occurred. We can use the item's state to bound the search.
                if (other.CurState != ItemState.NotYetInserted) break;
                if (other.OpId == newItem.RightParent) throw new InvalidOperationException("Invalid state");

                // The index of the origin left / right for the other item.
                int oleftIdx = other.OriginLeft == -1 ? -1 : Utils.FindItemIdx(ctx, other.OriginLeft);
                if (oleftIdx < leftIdx) break;
                else if (oleftIdx == leftIdx)
                {
                    int orightIdx = other.RightParent == -1 ? ctx.Items.Count : Utils.FindItemIdx(ctx, other.RightParent);

                    if (orightIdx == rightIdx && Utils.LvCmp(cg, newItem.OpId, other.OpId) < 0)
                    {
                        break;
                    }
                    else scanning = orightIdx < rightIdx;
                }

                scanEndPos += Utils.ItemWidth(other.EndState);
                scanIdx++;

                if (!scanning)
                {
                    cursor.Idx = scanIdx;
                    cursor.EndPos = scanEndPos;
                }
            }

            // We've found the position. Insert where the cursor points.
        }

        private static DocCursor FindByCurPos<T>(EditContext<T> ctx, int targetPos)
        {
            int curPos = 0;
            int endPos = 0;
            int i = 0;

            while (curPos < targetPos)
            {
                if (i >= ctx.Items.Count) throw new InvalidOperationException("Document is not long enough to find targetPos");

                var item = ctx.Items[i];
                curPos += Utils.ItemWidth(item.CurState);
                endPos += Utils.ItemWidth(item.EndState);
                i++;
            }

            return new DocCursor { Idx = i, EndPos = endPos };
        }

        public static void MergeChangesIntoBranch<T>(Branch<T> branch, ListOpLog<T> oplog)
        {
            var ctx = new EditContext<T>
            {
                Items = new List<Item>(),
                DelTargets = Enumerable.Repeat(-1, oplog.Ops.Count).ToList(),
                ItemsByLV = Enumerable.Repeat<Item>(null, oplog.Ops.Count).ToList(),
                CurVersion = new List<int>(branch.Version ?? new List<int>())
            };

            // Initialize the snapshot and items based on the branch's current state
            var snapshot = new List<T>(branch.Snapshot);

            // Determine the versions to traverse
            int fromOp = 0; // Start from the beginning
            int toOp = Utils.NextLV(oplog.Cg);

            // Traverse and apply new operations from oplog
            TraverseAndApply(ctx, oplog, snapshot, fromOp, toOp);

            // Update the branch
            branch.Snapshot = snapshot;
            branch.Version = new List<int>(oplog.Cg.Heads);
        }
    }
}