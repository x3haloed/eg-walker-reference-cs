namespace EgWalkerReference
{
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
            if (cg.Entries.Count == 0) return 0;
            return cg.Entries.Max(e => e.VEnd);
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

        public static CGEntry AddRaw(CausalGraph cg, RawVersion id, int len = 1, List<RawVersion> rawParents = null, int? version = null)
        {
            List<int> parents = rawParents != null
                ? RawToLVList(cg, rawParents)
                : new List<int>(cg.Heads);

            return Add(cg, id.Agent, id.Seq, id.Seq + len, parents, version);
        }


        public static CGEntry Add(CausalGraph cg, string agent, int seqStart, int seqEnd, List<int> parents, int? version = null)
        {
            if (!version.HasValue)
            {
                version = NextLV(cg);

                while (true)
                {
                    var existingEntry = FindClientEntryTrimmed(cg, agent, seqStart);
                    if (existingEntry == null) break; // Insert start..end.

                    if (existingEntry.SeqEnd >= seqEnd) return null; // The entire span was already inserted.

                    // Or trim and loop.
                    seqStart = existingEntry.SeqEnd;
                    parents = new List<int> { existingEntry.Version + (existingEntry.SeqEnd - existingEntry.Seq) - 1 };
                }
            }
            else
            {
                // Ensure the version doesn't conflict with existing entries
                if (cg.Entries.Any(e => (version.Value >= e.Version && version.Value < e.VEnd)))
                {
                    throw new InvalidOperationException($"Version {version.Value} already exists in the causal graph.");
                }
            }

            int len = seqEnd - seqStart;
            int vEnd = version.Value + len;
            var entry = new CGEntry
            {
                Version = version.Value,
                VEnd = vEnd,
                Agent = agent,
                Seq = seqStart,
                Parents = new List<int>(parents)
            };

            // Insert the entry into the causal graph
            InsertCGEntry(cg, entry);

            // Insert into AgentToVersion mapping
            var clientEntry = new ClientEntry
            {
                Seq = seqStart,
                SeqEnd = seqEnd,
                Version = version.Value
            };
            var agentEntries = ClientEntriesForAgent(cg, agent);
            InsertClientEntry(agentEntries, clientEntry);

            // Update heads
            cg.Heads = AdvanceFrontier(cg.Heads, vEnd - 1, parents);

            return entry;
        }

        public static CGEntry FindEntryContainingRaw(CausalGraph cg, int v)
        {
            int idx = BinarySearch(cg.Entries, (entry) =>
            {
                if (v < entry.Version)
                    return -1;
                else if (v >= entry.VEnd)
                    return 1;
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

        public static int LvCmp(CausalGraph cg, int a, int b)
        {
            return LvToRaw(cg, a).CompareTo(LvToRaw(cg, b));
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
            var queue = new SortedSet<(List<int> V, DiffFlag Flag)>(Comparer<(List<int> V, DiffFlag Flag)>.Create((x, y) =>
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
            }))
            {
                (a.OrderByDescending(v => v).ToList(), DiffFlag.A),
                (b.OrderByDescending(v => v).ToList(), DiffFlag.B)
            };

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

        /// <summary>
        /// Two versions have one of 4 different relationship configurations:
        /// - They're equal (a == b)
        /// - They're concurrent (a || b)
        /// - Or one dominates the other (a < b or b > a).
        /// 
        /// This method depends on the caller to check if the passed versions are equal
        /// (a == b). Otherwise it returns 0 if the operations are concurrent,
        /// -1 if a < b or 1 if b > a.
        /// </summary>
        /// <exception cref="InvalidOperationException"></exception>
        public static int CompareVersions(CausalGraph cg, int a, int b)
        {
            if (a > b)
            {
                return VersionContainsLV(cg, new List<int> { a }, b) ? -1 : 0;
            }
            else if (a < b)
            {
                return VersionContainsLV(cg, new List<int> { b }, a) ? 1 : 0;
            }
            
            throw new InvalidOperationException("a and b are equal");
        }

        public class PartialSerializedCGEntry
        {
            public int Version;
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
                        Version = e.Version + offset,
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
            if (data == null || data.Count == 0)
            {
                // No data to merge; return an empty LVRange or handle as needed
                return new LVRange(0, 0);
            }

            int? start = null;
            int? end = null;

            foreach (var entry in data)
            {
                // Convert parents from RawVersion to local versions
                List<int> parents = RawToLVList(cg, entry.Parents);

                int version = entry.Version;
                int vEnd = version + entry.Len;

                // Update start and end
                if (start == null || entry.Version < start) start = entry.Version;
                if (end == null || (entry.Version + entry.Len) > end) end = entry.Version + entry.Len;

                // Create the CGEntry directly
                var cgEntry = new CGEntry
                {
                    Version = version,
                    VEnd = vEnd,
                    Agent = entry.Agent,
                    Seq = entry.Seq,
                    Parents = parents
                };

                // Insert the entry into the causal graph
                InsertCGEntry(cg, cgEntry);

                // Update AgentToVersion
                var clientEntry = new ClientEntry
                {
                    Seq = entry.Seq,
                    SeqEnd = entry.Seq + entry.Len,
                    Version = version
                };

                var agentEntries = ClientEntriesForAgent(cg, entry.Agent);
                InsertClientEntry(agentEntries, clientEntry);
            }

            // Return the range of versions that were merged
            return new LVRange(start.Value, end.Value);
        }

        private static void InsertCGEntry(CausalGraph cg, CGEntry newEntry)
        {
            int idx = cg.Entries.BinarySearch(newEntry, Comparer<CGEntry>.Create((a, b) => a.Version.CompareTo(b.Version)));
            if (idx < 0) idx = ~idx;
            cg.Entries.Insert(idx, newEntry);
        }

        private static void InsertClientEntry(List<ClientEntry> entries, ClientEntry newEntry)
        {
            int idx = entries.BinarySearch(newEntry, Comparer<ClientEntry>.Create((a, b) => a.Seq.CompareTo(b.Seq)));
            if (idx < 0) idx = ~idx;
            entries.Insert(idx, newEntry);
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

        public static int FindItemIdx<T>(EditContext<T> ctx, int needle)
        {
            int idx = ctx.Items.FindIndex(i => i.OpId == needle);
            if (idx == -1) throw new InvalidOperationException("Could not find needle in items");
            return idx;
        }

        public static int ItemWidth(ItemState state)
        {
            return state == ItemState.Inserted ? 1 : 0;
        }

        public static VersionSummary SummarizeVersion(CausalGraph cg)
        {
            var summary = new VersionSummary();

            foreach (var kvp in cg.AgentToVersion)
            {
                string agent = kvp.Key;
                var clientEntries = kvp.Value;
                var ranges = new List<LVRange>();

                foreach (var ce in clientEntries)
                {
                    // Each ClientEntry represents a range of sequences for the agent
                    ranges.Add(new LVRange(ce.Seq, ce.SeqEnd));
                }

                summary[agent] = ranges;
            }

            return summary;
        }
    }
}