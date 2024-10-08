using EgWalkerReference;

// Create a new ListOpLog for first oplog
var oplog1 = ListOperations.CreateOpLog<string>();

// Insert 'h', 'i' from user1
ListOperations.LocalInsert(oplog1, "user1", 0, "h", "i");
Console.WriteLine(ListOperations.CheckoutSimpleString(oplog1)); // Should print 'hi'

// Users 1 and 2 concurrently insert 'A' and 'B' at the start of a new document
var oplog2 = ListOperations.CreateOpLog<string>();

var v = ListOperations.GetLatestVersion(oplog2); // Empty version in this case
ListOperations.PushOp(oplog2, new RawVersion("user1", 0), v, "ins", 0, "A");
ListOperations.PushOp(oplog2, new RawVersion("user2", 0), v, "ins", 0, "B");

// Prints 'AB' since the tie breaks by ordering by agent ID
Console.WriteLine(ListOperations.CheckoutSimpleString(oplog2)); // Should print 'AB'

// Now, let's simulate the same thing using 2 oplogs
var oplogA = ListOperations.CreateOpLog<string>();
ListOperations.LocalInsert(oplogA, "user1", 0, "A");

var oplogB = ListOperations.CreateOpLog<string>();
ListOperations.LocalInsert(oplogB, "user2", 0, "B");

// The two users sync changes:
ListOperations.MergeOplogInto(oplogA, oplogB);
ListOperations.MergeOplogInto(oplogB, oplogA);

// And now they both see 'AB'
Console.WriteLine(ListOperations.CheckoutSimpleString(oplogA)); // Should print 'AB'
Console.WriteLine(ListOperations.CheckoutSimpleString(oplogB)); // Should print 'AB'

// Finally, let's make a branch and update it
var branch = new Branch<string> { Snapshot = new List<string>() };
ListOperations.MergeChangesIntoBranch(branch, oplogA);
Console.WriteLine(string.Join(",", branch.Snapshot)); // Should print 'A,B'