using System;
using System.Collections.Generic;
using System.Linq;

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
    
    public struct LVRange
    {
        public int Start;
        public int End;

        public LVRange(int start, int end)
        {
            Start = start;
            End = end;
        }
    }

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

    public class VersionSummary : Dictionary<string, List<LVRange>>
    {
    }

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

    public class ClientEntry
    {
        public int Seq;
        public int SeqEnd;
        public int Version;
    }

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

    public enum ItemState
    {
        NotYetInserted = 0,
        Inserted = 1,
        Deleted = 2
    }

    public class Item
    {
        public ItemState CurState { get; set; }
        public ItemState EndState { get; set; }
        public int OpId { get; set; }
        public int OriginLeft { get; set; }
        public int RightParent { get; set; }
    }

    public class EditContext<T>
    {
        public List<Item> Items { get; set; }
        public List<int> DelTargets { get; set; }
        public List<Item> ItemsByLV { get; set; }
        public List<int> CurVersion { get; set; }
    }

    public struct DocCursor
    {
        public int Idx;
        public int EndPos;
    }

    public static class Utils
    {
        public static void Assert(bool condition, string message)
        {
            if (!condition)
                throw new InvalidOperationException(message);
        }

        public static void AssertEq<T>(T a, T b, string message)
        {
            if (!EqualityComparer<T>.Default.Equals(a, b))
                throw new InvalidOperationException(message);
        }

        public static void PushRLEList<T>(List<T> list, T newItem, Func<T, T, bool> tryAppend)
        {
            if (list.Count == 0 || !tryAppend(list[list.Count - 1], newItem))
            {
                list.Add(newItem);
            }
        }

        public static void InsertRLEList<T>(List<T> list, T newItem, Func<T, int> getKey, Func<T, T, bool> tryAppend)
        {
            int newKey = getKey(newItem);
            if (list.Count == 0 || newKey >= getKey(list[list.Count - 1]))
            {
                // Common case. Just push the new entry to the end of the list like normal.
                PushRLEList(list, newItem, tryAppend);
            }
            else
            {
                // We need to insert the new entry. Find the index of the previous entry...
                int idx = BinarySearch(list, newKey, getKey);
                if (idx >= 0) throw new InvalidOperationException("Invalid state - item already exists");

                idx = ~idx; // The destination index is the complement of the returned index.

                // Try to append.
                if (idx == 0 || !tryAppend(list[idx - 1], newItem))
                {
                    // No good! Insert.
                    list.Insert(idx, newItem);
                }
            }
        }

        public static int BinarySearch<T>(List<T> list, int needle, Func<T, int> getKey)
        {
            int min = 0;
            int max = list.Count - 1;

            while (min <= max)
            {
                int mid = (min + max) / 2;
                int midKey = getKey(list[mid]);
                if (midKey < needle)
                    min = mid + 1;
                else if (midKey > needle)
                    max = mid - 1;
                else
                    return mid; // Found
            }
            return ~min; // Not found, return bitwise complement of insertion point
        }

        public static bool TryRangeAppend(LVRange r1, LVRange r2)
        {
            if (r1.End == r2.Start)
            {
                r1.End = r2.End;
                return true;
            }
            else return false;
        }

        public static bool TryRevRangeAppend(LVRange r1, LVRange r2)
        {
            if (r1.Start == r2.End)
            {
                r1.Start = r2.Start;
                return true;
            }
            else return false;
        }

        public static List<int> SortVersions(List<int> v)
        {
            var sorted = new List<int>(v);
            sorted.Sort();
            return sorted;
        }

        public static List<int> AdvanceFrontier(List<int> frontier, int vLast, List<int> parents)
        {
            var f = new List<int>();
            foreach (var v in frontier)
            {
                if (!parents.Contains(v))
                    f.Add(v);
            }
            f.Add(vLast);
            return SortVersions(f);
        }

        public static List<ClientEntry> ClientEntriesForAgent(CausalGraph cg, string agent)
        {
            if (!cg.AgentToVersion.TryGetValue(agent, out var entries))
            {
                entries = new List<ClientEntry>();
                cg.AgentToVersion[agent] = entries;
            }
            return entries;
        }

        public static V LastOr<T, V>(List<T> list, Func<T, V> f, V def)
        {
            return list.Count == 0 ? def : f(list[list.Count - 1]);
        }

        public static int NextLV(CausalGraph cg)
        {
            return LastOr(cg.Entries, e => e.VEnd, 0);
        }

        public static int NextSeqForAgent(CausalGraph cg, string agent)
        {
            if (cg.AgentToVersion.TryGetValue(agent, out var entries) && entries.Count > 0)
            {
                return entries[entries.Count - 1].SeqEnd;
            }
            return 0;
        }

        public static bool TryAppendEntries(CGEntry a, CGEntry b)
        {
            bool canAppend = b.Version == a.VEnd
                && a.Agent == b.Agent
                && a.Seq + (a.VEnd - a.Version) == b.Seq
                && b.Parents.Count == 1 && b.Parents[0] == a.VEnd - 1;

            if (canAppend)
            {
                a.VEnd = b.VEnd;
            }

            return canAppend;
        }

        public static bool TryAppendClientEntry(ClientEntry a, ClientEntry b)
        {
            bool canAppend = b.Seq == a.SeqEnd
                && b.Version == (a.Version + (a.SeqEnd - a.Seq));

            if (canAppend)
            {
                a.SeqEnd = b.SeqEnd;
            }
            return canAppend;
        }

        public static int BinarySearch<T>(List<T> list, Func<T, int> comparator)
        {
            int min = 0;
            int max = list.Count - 1;

            while (min <= max)
            {
                int mid = (min + max) / 2;
                int compareResult = comparator(list[mid]);

                if (compareResult > 0)
                    min = mid + 1;
                else if (compareResult < 0)
                    max = mid - 1;
                else
                    return mid;
            }
            return ~min;
        }

        public static ClientEntry FindClientEntryRaw(CausalGraph cg, string agent, int seq)
        {
            if (!cg.AgentToVersion.TryGetValue(agent, out var av))
                return null;

            int idx = BinarySearch(av, (entry) =>
            {
                if (seq < entry.Seq)
                    return 1;
                else if (seq >= entry.SeqEnd)
                    return -1;
                else
                    return 0;
            });

            return idx < 0 ? null : av[idx];
        }

        public static Tuple<ClientEntry, int> FindClientEntry(CausalGraph cg, string agent, int seq)
        {
            var clientEntry = FindClientEntryRaw(cg, agent, seq);
            return clientEntry == null ? null : Tuple.Create(clientEntry, seq - clientEntry.Seq);
        }

        public static ClientEntry FindClientEntryTrimmed(CausalGraph cg, string agent, int seq)
        {
            var result = FindClientEntry(cg, agent, seq);
            if (result == null) return null;

            var clientEntry = result.Item1;
            var offset = result.Item2;
            if (offset == 0)
            {
                return clientEntry;
            }
            else
            {
                return new ClientEntry
                {
                    Seq = seq,
                    SeqEnd = clientEntry.SeqEnd,
                    Version = clientEntry.Version + offset
                };
            }
        }

        public static bool HasVersion(CausalGraph cg, string agent, int seq)
        {
            return FindClientEntryRaw(cg, agent, seq) != null;
        }

        public static CGEntry AddRaw(CausalGraph cg, RawVersion id, int len = 1, List<RawVersion> rawParents = null)
        {
            List<int> parents = rawParents != null
                ? RawToLVList(cg, rawParents)
                : new List<int>(cg.Heads);

            return Add(cg, id.Agent, id.Seq, id.Seq + len, parents);
        }

        public static CGEntry Add(CausalGraph cg, string agent, int seqStart, int seqEnd, List<int> parents)
        {
            int version = NextLV(cg);

            while (true)
            {
                var existingEntry = FindClientEntryTrimmed(cg, agent, seqStart);
                if (existingEntry == null) break; // Insert start..end.

                if (existingEntry.SeqEnd >= seqEnd) return null; // The entire span was already inserted.

                // Or trim and loop.
                seqStart = existingEntry.SeqEnd;
                parents = new List<int> { existingEntry.Version + (existingEntry.SeqEnd - existingEntry.Seq) - 1 };
            }

            int len = seqEnd - seqStart;
            int vEnd = version + len;
            var entry = new CGEntry
            {
                Version = version,
                VEnd = vEnd,
                Agent = agent,
                Seq = seqStart,
                Parents = new List<int>(parents)
            };

            // The entry list will remain ordered here in standard version order.
            PushRLEList(cg.Entries, entry, TryAppendEntries);
            // But the agent entries may end up out of order, since we might get [b,0] before [b,1] if
            // the same agent modifies two different branches. Hence, insertRLEList instead of pushRLEList.
            InsertRLEList(
                ClientEntriesForAgent(cg, agent),
                new ClientEntry { Seq = seqStart, SeqEnd = seqEnd, Version = version },
                e => e.Seq,
                TryAppendClientEntry
            );

            cg.Heads = AdvanceFrontier(cg.Heads, vEnd - 1, parents);
            return entry;
        }

        public static CGEntry FindEntryContainingRaw(CausalGraph cg, int v)
        {
            int idx = BinarySearch(cg.Entries, (entry) =>
            {
                if (v < entry.Version)
                    return 1;
                else if (v >= entry.VEnd)
                    return -1;
                else
                    return 0;
            });

            if (idx < 0) throw new InvalidOperationException("Invalid or unknown local version " + v);
            return cg.Entries[idx];
        }

        public static Tuple<CGEntry, int> FindEntryContaining(CausalGraph cg, int v)
        {
            var e = FindEntryContainingRaw(cg, v);
            int offset = v - e.Version;
            return Tuple.Create(e, offset);
        }

        public static Tuple<string, int, List<int>> LvToRawWithParents(CausalGraph cg, int v)
        {
            var result = FindEntryContaining(cg, v);
            var e = result.Item1;
            var offset = result.Item2;
            List<int> parents = offset == 0 ? new List<int>(e.Parents) : new List<int> { v - 1 };
            return Tuple.Create(e.Agent, e.Seq + offset, parents);
        }

        public static RawVersion LvToRaw(CausalGraph cg, int v)
        {
            var result = FindEntryContaining(cg, v);
            var e = result.Item1;
            var offset = result.Item2;
            return new RawVersion(e.Agent, e.Seq + offset);
        }

        public static List<RawVersion> LvToRawList(CausalGraph cg, List<int> parents = null)
        {
            if (parents == null) parents = new List<int>(cg.Heads);
            var result = new List<RawVersion>();
            foreach (var v in parents)
            {
                result.Add(LvToRaw(cg, v));
            }
            return result;
        }

        public static int RawToLV(CausalGraph cg, string agent, int seq)
        {
            var clientEntry = FindClientEntryTrimmed(cg, agent, seq);
            if (clientEntry == null) throw new InvalidOperationException($"Unknown ID: ({agent}, {seq})");
            return clientEntry.Version;
        }

        public static List<int> RawToLVList(CausalGraph cg, List<RawVersion> parents)
        {
            var result = new List<int>();
            foreach (var rv in parents)
            {
                result.Add(RawToLV(cg, rv.Agent, rv.Seq));
            }
            return result;
        }

        public static IEnumerable<CGEntry> IterVersionsBetween(CausalGraph cg, int vStart, int vEnd)
        {
            if (vStart == vEnd) yield break;

            int idx = BinarySearch(cg.Entries, (entry) =>
            {
                if (vStart < entry.Version)
                    return 1;
                else if (vStart >= entry.VEnd)
                    return -1;
                else
                    return 0;
            });

            if (idx < 0) throw new InvalidOperationException("Invalid or missing version: " + vStart);

            for (; idx < cg.Entries.Count; idx++)
            {
                var entry = cg.Entries[idx];
                if (entry.Version >= vEnd) break;

                if (vStart <= entry.Version && vEnd >= entry.VEnd)
                {
                    if (entry.Version == entry.VEnd) throw new InvalidOperationException("Invalid state");
                    yield return entry; // Keep the entire entry.
                }
                else
                {
                    // Slice the entry by vStart / vEnd.
                    int vLocalStart = Math.Max(vStart, entry.Version);
                    int vLocalEnd = Math.Min(vEnd, entry.VEnd);

                    if (vLocalStart == vLocalEnd) throw new InvalidOperationException("Invalid state");

                    var newEntry = new CGEntry
                    {
                        Version = vLocalStart,
                        VEnd = vLocalEnd,
                        Agent = entry.Agent,
                        Seq = entry.Seq + (vLocalStart - entry.Version),
                        Parents = vLocalStart == entry.Version ? new List<int>(entry.Parents) : new List<int> { vLocalStart - 1 }
                    };

                    yield return newEntry;
                }
            }
        }

        public static void IntersectWithSummaryFull(CausalGraph cg, VersionSummary summary, Action<string, int, int, int> visit)
        {
            foreach (var kvp in summary)
            {
                string agent = kvp.Key;
                var ranges = kvp.Value;

                if (!cg.AgentToVersion.TryGetValue(agent, out var clientEntries))
                {
                    clientEntries = null;
                }

                foreach (var range in ranges)
                {
                    int startSeq = range.Start;
                    int endSeq = range.End;

                    if (clientEntries != null)
                    {
                        int idx = BinarySearch(clientEntries, (entry) =>
                        {
                            if (startSeq < entry.Seq)
                                return 1;
                            else if (startSeq >= entry.SeqEnd)
                                return -1;
                            else
                                return 0;
                        });

                        if (idx < 0) idx = ~idx;

                        for (; idx < clientEntries.Count; idx++)
                        {
                            var ce = clientEntries[idx];
                            if (ce.Seq >= endSeq) break;

                            if (ce.Seq > startSeq)
                            {
                                visit(agent, startSeq, ce.Seq, -1);
                                startSeq = ce.Seq;
                            }

                            int seqOffset = startSeq - ce.Seq;
                            int versionStart = ce.Version + seqOffset;

                            int localSeqEnd = Math.Min(ce.SeqEnd, endSeq);

                            visit(agent, startSeq, localSeqEnd, versionStart);

                            startSeq = localSeqEnd;
                        }
                    }

                    if (startSeq < endSeq)
                    {
                        visit(agent, startSeq, endSeq, -1);
                    }
                }
            }
        }

        public static Tuple<List<int>, VersionSummary> IntersectWithSummary(CausalGraph cg, VersionSummary summary, List<int> versionsIn = null)
        {
            if (versionsIn == null) versionsIn = new List<int>();
            VersionSummary remainder = null;
            var versions = new List<int>(versionsIn);

            IntersectWithSummaryFull(cg, summary, (agent, startSeq, endSeq, versionStart) =>
            {
                if (versionStart >= 0)
                {
                    int versionEnd = versionStart + (endSeq - startSeq);
                    EachVersionBetween(cg, versionStart, versionEnd, (e, vs, ve) =>
                    {
                        int vLast = ve - 1;
                        if (vLast < e.Version) throw new InvalidOperationException("Invalid state");
                        versions.Add(vLast);
                    });
                }
                else
                {
                    remainder ??= new VersionSummary();
                    if (!remainder.TryGetValue(agent, out var list))
                    {
                        list = new List<LVRange>();
                        remainder[agent] = list;
                    }
                    list.Add(new LVRange(startSeq, endSeq));
                }
            });

            return Tuple.Create(FindDominators(cg, versions), remainder);
        }

        public static void PushReversedRLE(List<LVRange> list, int start, int end)
        {
            PushRLEList(list, new LVRange(start, end), TryRevRangeAppend);
        }

        public enum DiffFlag { A = 0, B = 1, Shared = 2 }

        public static DiffResult Diff(CausalGraph cg, List<int> a, List<int> b)
        {
            var flags = new Dictionary<int, DiffFlag>();
            var queue = new SortedSet<int>(Comparer<int>.Create((x, y) => y.CompareTo(x)));

            int numShared = 0;

            void Enqueue(int v, DiffFlag flag)
            {
                if (!flags.TryGetValue(v, out var currentType))
                {
                    queue.Add(v);
                    flags[v] = flag;
                    if (flag == DiffFlag.Shared) numShared++;
                }
                else if (flag != currentType && currentType != DiffFlag.Shared)
                {
                    flags[v] = DiffFlag.Shared;
                    numShared++;
                }
            }

            foreach (var v in a) Enqueue(v, DiffFlag.A);
            foreach (var v in b) Enqueue(v, DiffFlag.B);

            var aOnly = new List<LVRange>();
            var bOnly = new List<LVRange>();

            void MarkRun(int start, int endInclusive, DiffFlag flag)
            {
                if (endInclusive < start) throw new InvalidOperationException("end < start");
                if (flag == DiffFlag.Shared) return;

                var target = flag == DiffFlag.A ? aOnly : bOnly;
                PushReversedRLE(target, start, endInclusive + 1);
            }

            while (queue.Count > numShared)
            {
                int v = queue.Min;
                queue.Remove(v);
                var flag = flags[v];

                if (flag == DiffFlag.Shared) numShared--;

                var e = FindEntryContainingRaw(cg, v);

                while (queue.Count > 0 && queue.Min >= e.Version)
                {
                    int v2 = queue.Min;
                    queue.Remove(v2);
                    var flag2 = flags[v2];

                    if (flag2 == DiffFlag.Shared) numShared--;

                    if (flag2 != flag)
                    {
                        MarkRun(v2 + 1, v, flag);
                        v = v2;
                        flag = DiffFlag.Shared;
                    }
                }

                MarkRun(e.Version, v, flag);

                foreach (var p in e.Parents)
                {
                    Enqueue(p, flag);
                }
            }

            aOnly.Reverse();
            bOnly.Reverse();
            return new DiffResult { AOnly = aOnly, BOnly = bOnly };
        }

        public static bool VersionContainsLV(CausalGraph cg, List<int> frontier, int target)
        {
            if (frontier.Contains(target)) return true;

            var queue = new SortedSet<int>(Comparer<int>.Create((x, y) => y.CompareTo(x)));
            foreach (var v in frontier)
            {
                if (v > target) queue.Add(v);
            }

            while (queue.Count > 0)
            {
                int v = queue.Min;
                queue.Remove(v);

                if (v == target) return true;

                var e = FindEntryContainingRaw(cg, v);
                if (e.Version <= target) return true;

                while (queue.Count > 0 && queue.Min >= e.Version)
                {
                    queue.Remove(queue.Min);
                }

                foreach (var p in e.Parents)
                {
                    if (p == target) return true;
                    else if (p > target) queue.Add(p);
                }
            }

            return false;
        }

        public static void FindDominators2(CausalGraph cg, List<int> versions, Action<int, bool> cb)
        {
            if (versions.Count == 0) return;
            else if (versions.Count == 1)
            {
                cb(versions[0], true);
                return;
            }
            else if (versions.Count == 2)
            {
                int v0 = versions[0], v1 = versions[1];
                if (v0 == v1)
                {
                    cb(v0, true);
                    cb(v0, false);
                }
                else
                {
                    if (v0 > v1) (v0, v1) = (v1, v0);
                    cb(v1, true);
                    cb(v0, !VersionContainsLV(cg, new List<int> { v1 }, v0));
                }
                return;
            }

            var queue = new SortedSet<int>(Comparer<int>.Create((x, y) => y.CompareTo(x)));
            foreach (var v in versions) queue.Add(v * 2);

            int inputsRemaining = versions.Count;

            while (queue.Count > 0 && inputsRemaining > 0)
            {
                int vEnc = queue.Min;
                queue.Remove(vEnc);

                bool isInput = (vEnc % 2) == 0;
                int v = vEnc >> 1;

                if (isInput)
                {
                    cb(v, true);
                    inputsRemaining -= 1;
                }

                var e = FindEntryContainingRaw(cg, v);

                while (queue.Count > 0 && queue.Min >= e.Version * 2)
                {
                    int v2Enc = queue.Min;
                    queue.Remove(v2Enc);
                    bool isInput2 = (v2Enc % 2) == 0;
                    if (isInput2)
                    {
                        cb(v2Enc >> 1, false);
                        inputsRemaining -= 1;
                    }
                }

                foreach (var p in e.Parents)
                {
                    queue.Add(p * 2 + 1);
                }
            }
        }

        public static List<int> FindDominators(CausalGraph cg, List<int> versions)
        {
            if (versions.Count <= 1) return versions;
            var result = new List<int>();
            FindDominators2(cg, versions, (v, isDominator) =>
            {
                if (isDominator) result.Add(v);
            });
            result.Reverse();
            return result;
        }

        public static bool LvEq(List<int> a, List<int> b)
        {
            if (a.Count != b.Count) return false;
            for (int i = 0; i < a.Count; i++)
            {
                if (a[i] != b[i]) return false;
            }
            return true;
        }

        public static List<int> FindConflicting(CausalGraph cg, List<int> a, List<int> b, Action<LVRange, DiffFlag> visit)
        {
            var queue = new SortedSet<(List<int> V, DiffFlag Flag)>(Comparer<(List<int>, DiffFlag)>.Create((x, y) =>
            {
                for (int i = 0; i < x.V.Count; i++)
                {
                    if (y.V.Count <= i) return -1;
                    int cmp = y.V[i].CompareTo(x.V[i]);
                    if (cmp != 0) return cmp;
                }
                if (x.V.Count < y.V.Count) return -1;
                if (x.V.Count > y.V.Count) return 1;
                return x.Flag.CompareTo(y.Flag);
            }));

            queue.Add((a.OrderByDescending(v => v).ToList(), DiffFlag.A));
            queue.Add((b.OrderByDescending(v => v).ToList(), DiffFlag.B));

            while (true)
            {
                if (queue.Count == 0) return new List<int>();

                var (v, flag) = queue.First();
                queue.Remove(queue.First());

                if (v.Count == 0) return new List<int>();

                while (queue.Count > 0)
                {
                    var (peekV, peekFlag) = queue.First();
                    if (LvEq(v, peekV))
                    {
                        if (peekFlag != flag) flag = DiffFlag.Shared;
                        queue.Remove(queue.First());
                    }
                    else
                    {
                        break;
                    }
                }

                if (queue.Count == 0) return v.Reverse<int>().ToList();

                if (v.Count > 1)
                {
                    for (int i = 1; i < v.Count; i++)
                    {
                        queue.Add((new List<int> { v[i] }, flag));
                    }
                }

                int t = v[0];
                var containingTxn = FindEntryContainingRaw(cg, t);
                int txnStart = containingTxn.Version;
                int end = t + 1;

                while (true)
                {
                    if (queue.Count == 0)
                    {
                        return new List<int> { end - 1 };
                    }
                    else
                    {
                        var (peekV, peekFlag) = queue.First();

                        if (peekV.Count >= 1 && peekV[0] >= txnStart)
                        {
                            queue.Remove(queue.First());

                            int peekLast = peekV[0];

                            if (peekLast + 1 < end)
                            {
                                visit(new LVRange(peekLast + 1, end), flag);
                                end = peekLast + 1;
                            }

                            if (peekFlag != flag) flag = DiffFlag.Shared;

                            if (peekV.Count > 1)
                            {
                                for (int i = 1; i < peekV.Count; i++)
                                {
                                    queue.Add((new List<int> { peekV[i] }, peekFlag));
                                }
                            }
                        }
                        else
                        {
                            visit(new LVRange(txnStart, end), flag);
                            queue.Add((containingTxn.Parents.OrderByDescending(p => p).ToList(), flag));
                            break;
                        }
                    }
                }
            }
        }

        public static int CompareVersions(CausalGraph cg, int a, int b)
        {
            if (a == b) return 0;
            else if (VersionContainsLV(cg, new List<int> { a }, b)) return 1; // a causally follows b
            else if (VersionContainsLV(cg, new List<int> { b }, a)) return -1; // b causally follows a
            else
            {
                // Concurrent versions; use a deterministic tie-breaker
                var rvA = LvToRaw(cg, a);
                var rvB = LvToRaw(cg, b);

                // Tie-breaker: compare agent IDs
                int agentComparison = string.Compare(rvA.Agent, rvB.Agent, StringComparison.Ordinal);
                if (agentComparison != 0)
                    return agentComparison;
                else
                    return rvA.Seq.CompareTo(rvB.Seq);
            }
        }


        public class PartialSerializedCGEntry
        {
            public string Agent;
            public int Seq;
            public int Len;
            public List<RawVersion> Parents;
        }

        public class PartialSerializedCG : List<PartialSerializedCGEntry> { }

        public static PartialSerializedCG SerializeDiff(CausalGraph cg, List<LVRange> ranges)
        {
            var entries = new PartialSerializedCG();
            foreach (var range in ranges)
            {
                int start = range.Start;
                int end = range.End;
                while (start != end)
                {
                    var result = FindEntryContaining(cg, start);
                    var e = result.Item1;
                    int offset = result.Item2;

                    int localEnd = Math.Min(end, e.VEnd);
                    int len = localEnd - start;
                    var parents = offset == 0
                        ? LvToRawList(cg, e.Parents)
                        : new List<RawVersion> { new RawVersion(e.Agent, e.Seq + offset - 1) };

                    entries.Add(new PartialSerializedCGEntry
                    {
                        Agent = e.Agent,
                        Seq = e.Seq + offset,
                        Len = len,
                        Parents = parents
                    });

                    start += len;
                }
            }
            return entries;
        }

        public static PartialSerializedCG SerializeFromVersion(CausalGraph cg, List<int> v)
        {
            var ranges = Diff(cg, v, cg.Heads).BOnly;
            return SerializeDiff(cg, ranges);
        }

        public static LVRange MergePartialVersions(CausalGraph cg, PartialSerializedCG data)
        {
            int start = NextLV(cg);

            foreach (var entry in data)
            {
                AddRaw(cg, new RawVersion(entry.Agent, entry.Seq), entry.Len, entry.Parents);
            }
            return new LVRange(start, NextLV(cg));
        }

        public static IEnumerable<CGEntry> MergePartialVersions2(CausalGraph cg, PartialSerializedCG data)
        {
            foreach (var entry in data)
            {
                var newEntry = AddRaw(cg, new RawVersion(entry.Agent, entry.Seq), entry.Len, entry.Parents);
                if (newEntry != null) yield return newEntry;
            }
        }

        public static List<int> AdvanceVersionFromSerialized(CausalGraph cg, PartialSerializedCG data, List<int> version)
        {
            foreach (var entry in data)
            {
                var parentLVs = RawToLVList(cg, entry.Parents);
                int vLast = RawToLV(cg, entry.Agent, entry.Seq + entry.Len - 1);
                version = AdvanceFrontier(version, vLast, parentLVs);
            }

            // Note: Callers might need to call FindDominators on the result.
            return version;
        }

        public static void CheckCG(CausalGraph cg)
        {
            foreach (var e in cg.Entries)
            {
                if (e.VEnd <= e.Version) throw new InvalidOperationException("Inverted versions in entry");
            }
            // Additional validation can be added here.
        }

        private static void EachVersionBetween(CausalGraph cg, int vStart, int vEnd, Action<CGEntry, int, int> visit)
        {
            int idx = BinarySearch(cg.Entries, (entry) =>
            {
                if (vStart < entry.Version)
                    return 1;
                else if (vStart >= entry.VEnd)
                    return -1;
                else
                    return 0;
            });

            if (idx < 0) throw new InvalidOperationException("Invalid or missing version: " + vStart);

            for (; idx < cg.Entries.Count; idx++)
            {
                var entry = cg.Entries[idx];
                if (entry.Version >= vEnd) break;

                int vs = Math.Max(vStart, entry.Version);
                int ve = Math.Min(vEnd, entry.VEnd);

                visit(entry, vs, ve);
            }
        }

        private static int FindItemIdx<T>(EditContext<T> ctx, int needle)
        {
            int idx = ctx.Items.FindIndex(i => i.OpId == needle);
            if (idx == -1) throw new InvalidOperationException("Could not find needle in items");
            return idx;
        }

        private static int ItemWidth(ItemState state)
        {
            return state == ItemState.Inserted ? 1 : 0;
        }
    }

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
            if (cursor.Idx >= ctx.Items.Count || ctx.Items[cursor.Idx].CurState != ItemState.NotYetInserted) return;

            bool scanning = false;
            int scanIdx = cursor.Idx;
            int scanEndPos = cursor.EndPos;

            int leftIdx = cursor.Idx - 1;
            int rightIdx = newItem.RightParent == -1 ? ctx.Items.Count : Utils.FindItemIdx(ctx, newItem.RightParent);

            while (scanIdx < ctx.Items.Count)
            {
                var other = ctx.Items[scanIdx];

                if (other.CurState != ItemState.NotYetInserted) break;
                if (other.OpId == newItem.RightParent) throw new InvalidOperationException("Invalid state");

                int oleftIdx = other.OriginLeft == -1 ? -1 : Utils.FindItemIdx(ctx, other.OriginLeft);
                if (oleftIdx < leftIdx) break;
                else if (oleftIdx == leftIdx)
                {
                    int orightIdx = other.RightParent == -1 ? ctx.Items.Count : Utils.FindItemIdx(ctx, other.RightParent);

                    if (orightIdx == rightIdx)
                    {
                        int cmp = Utils.CompareVersions(cg, newItem.OpId, other.OpId);
                        if (cmp < 0)
                        {
                            // newItem should come before other
                            break;
                        }
                        else if (cmp > 0)
                        {
                            // Continue scanning
                            scanning = orightIdx < rightIdx;
                        }
                        else
                        {
                            // Tie-breaker resulted in equality, which shouldn't happen
                            throw new InvalidOperationException("Tie-breaker did not resolve ordering");
                        }
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