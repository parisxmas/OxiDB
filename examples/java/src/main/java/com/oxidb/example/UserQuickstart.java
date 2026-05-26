package com.oxidb.example;

import com.oxidb.client.HelloResponse;
import com.oxidb.client.OxiDbClient;
import com.oxidb.client.OxiDbException;
import com.oxidb.client.Query;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Minimal end-to-end demo. Walks through every public surface of the
 * Java client against a local oxidb-server. Run with:
 *
 * <pre>
 *   # in one terminal:
 *   ./oxidb-server                       # listens on 127.0.0.1:4444
 *
 *   # in another:
 *   cd examples/java
 *   mvn -q compile exec:java
 * </pre>
 */
public class UserQuickstart {

    public record User(long _id, String name, int age, boolean active, String department) {}

    public static void main(String[] args) throws Exception {
        String host = System.getenv().getOrDefault("OXIDB_HOST", "127.0.0.1");
        int port = Integer.parseInt(System.getenv().getOrDefault("OXIDB_PORT", "4444"));
        String collection = "demo_users";

        try (OxiDbClient client = OxiDbClient.connect(host, port)) {

            // ── 1. HELLO handshake ────────────────────────────────────
            HelloResponse hello = client.hello("oxidb-java-quickstart/1.0");
            System.out.println("Connected to " + hello.name() + " " + hello.version()
                + " (wire v" + hello.wireVersion()
                + ", stable surface v" + hello.stableSurfaceVersion() + ")");
            System.out.println("  features: " + hello.features());
            System.out.println("  auth methods: " + hello.authMethods());

            // Feature gates — only call optional commands if the server advertises them.
            if (hello.hasFeature("scram_sha_256")) {
                System.out.println("  → SCRAM is available; would call client.authSimple(...) if auth_enabled.");
            }

            // ── 2. Clean slate ────────────────────────────────────────
            // Best-effort delete of any prior demo run. We ignore NotFound
            // errors that fire when the collection doesn't exist yet.
            try {
                client.delete(collection, Map.of());
            } catch (OxiDbException.OxiDbNotFoundException ignored) { }

            // ── 3. Insert a single doc, get its id ────────────────────
            long aliceId = client.insertReturningId(collection, Map.of(
                "name", "Alice",
                "age", 30,
                "active", true,
                "department", "Engineering"
            ));
            System.out.println("\nInserted Alice with _id=" + aliceId);

            // ── 4. Insert many in one shot ────────────────────────────
            long[] ids = client.insertManyReturningIds(collection, List.of(
                Map.of("name", "Bob", "age", 25, "active", true, "department", "Sales"),
                Map.of("name", "Carol", "age", 35, "active", false, "department", "Engineering"),
                Map.of("name", "Dave", "age", 42, "active", true, "department", "Engineering"),
                Map.of("name", "Eve", "age", 19, "active", true, "department", "Sales")
            ));
            System.out.println("Inserted 4 more with ids=" + java.util.Arrays.toString(ids));

            // ── 5. Count ──────────────────────────────────────────────
            int total = client.count(collection, null);
            System.out.println("\nTotal users: " + total);

            // ── 6. Find with Query builder, typed deserialization ─────
            Map<String, Object> activeAdultEngineers = Query.and(
                Query.eq("active", true),
                Query.gte("age", 18),
                Query.eq("department", "Engineering")
            );
            List<User> engineers = client.find(collection, activeAdultEngineers, User.class);
            System.out.println("\nActive adult engineers (" + engineers.size() + "):");
            for (User u : engineers) {
                System.out.println("  - " + u.name() + " (" + u.age() + ")");
            }

            // ── 7. Range query ────────────────────────────────────────
            Map<String, Object> twentySomethings = Query.range("age", 20, 30);
            List<User> twenties = client.find(collection, twentySomethings, User.class);
            System.out.println("\nUsers in their 20s (" + twenties.size() + "): "
                + twenties.stream().map(User::name).toList());

            // ── 8. $in query ──────────────────────────────────────────
            Map<String, Object> deptIn = Query.in("department",
                List.of("Engineering", "Sales"));
            int eitherDept = client.count(collection, deptIn);
            System.out.println("Engineering OR Sales: " + eitherDept);

            // ── 9. findOne (typed, single result) ─────────────────────
            User alice = client.findOne(collection, Query.eq("name", "Alice"), User.class);
            System.out.println("\nFound Alice: " + alice);

            // ── 10. Async API (CompletableFuture) ─────────────────────
            CompletableFuture<List<User>> asyncEngineers =
                client.findAsync(collection, Query.eq("department", "Engineering"), User.class);
            List<User> result = asyncEngineers.join();
            System.out.println("Async result: " + result.size() + " engineers");

            // ── 11. Update ────────────────────────────────────────────
            int modified = client.update(collection,
                Query.eq("name", "Carol"),
                Map.of("$set", Map.of("active", true)));
            System.out.println("\nReactivated Carol: modified=" + modified + " doc(s)");

            // ── 12. Streaming (Iterable, paginated) ───────────────────
            // Useful for huge result sets that wouldn't fit in memory.
            System.out.println("\nStreaming all users (batch=2):");
            int seen = 0;
            for (User u : client.stream(collection, null,
                    Map.of("_id", 1), 2, User.class)) {
                System.out.println("  [" + (++seen) + "] " + u.name());
            }

            // ── 13. Exception types ───────────────────────────────────
            try {
                // Force a NotFound by querying a never-created collection
                client.count("no_such_collection_zzz", null);
            } catch (OxiDbException.OxiDbNotFoundException nf) {
                System.out.println("\nCaught typed exception: NotFound — " + nf.getServerMessage());
            } catch (OxiDbException other) {
                System.out.println("\nCaught generic OxiDbException — " + other.getServerMessage());
            }

            // ── 14. Cleanup ───────────────────────────────────────────
            int deleted = client.delete(collection, Map.of());
            System.out.println("\nCleanup: deleted " + deleted + " docs.");

            System.out.println("\nDone.");
        }
    }
}
