package com.oxidb.embedded;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * SharedPreferences-shaped key-value store over an embedded collection:
 * one document per key ({@code {k, v}}, unique index on {@code k}), every
 * write a single upsert, every read an indexed point lookup. Values are
 * anything JSON representable. Use it where you would reach for
 * SharedPreferences but want the data inside the (optionally encrypted)
 * OxiDB store next to the rest of the app's documents — with
 * {@link OxiDb#OxiDb(String, byte[])} the whole store is AES-256-GCM
 * encrypted at rest, key held in the Android Keystore.
 *
 * <pre>
 * OxiDb db = new OxiDb(context.getFilesDir() + "/oxidb_data", keyFromKeystore);
 * Preferences prefs = new Preferences(db);
 * prefs.put("theme", "dark");
 * String theme = prefs.getString("theme", "light");
 * </pre>
 *
 * Unlike SharedPreferences there is no editor/apply cycle: each put is a
 * durable write (WAL-backed) the moment it returns.
 */
public class Preferences {

    private final OxiDb db;
    private final String collection;

    /** Preferences in the default {@code _prefs} collection. */
    public Preferences(OxiDb db) {
        this(db, "_prefs");
    }

    /** A named namespace — several independent Preferences per database. */
    public Preferences(OxiDb db, String collection) {
        this.db = db;
        this.collection = collection;
        try {
            db.createUniqueIndex(collection, "k");
        } catch (RuntimeException ignored) {
            // Index already exists (or the collection is mid-creation
            // elsewhere) — correctness does not depend on it: every write
            // goes through a keyed upsert, the index only speeds reads.
        }
    }

    /** Insert-or-replace. One upsert: no read-modify-write race. */
    public void put(String key, Object value) {
        db.updateOne(
                collection,
                OxiDb.obj("k", key),
                OxiDb.obj("$set", OxiDb.obj("v", value)),
                true);
    }

    /** The stored value, or null. */
    public Object get(String key) {
        JSONObject doc = db.findOne(collection, OxiDb.obj("k", key));
        return doc == null ? null : doc.opt("v");
    }

    public String getString(String key, String def) {
        Object v = get(key);
        return v instanceof String ? (String) v : def;
    }

    public int getInt(String key, int def) {
        Object v = get(key);
        return v instanceof Number ? ((Number) v).intValue() : def;
    }

    public long getLong(String key, long def) {
        Object v = get(key);
        return v instanceof Number ? ((Number) v).longValue() : def;
    }

    public double getDouble(String key, double def) {
        Object v = get(key);
        return v instanceof Number ? ((Number) v).doubleValue() : def;
    }

    public boolean getBoolean(String key, boolean def) {
        Object v = get(key);
        return v instanceof Boolean ? (Boolean) v : def;
    }

    public boolean contains(String key) {
        return db.findOne(collection, OxiDb.obj("k", key)) != null;
    }

    /** Remove a key; true when something was removed. */
    public boolean remove(String key) {
        JSONObject r = db.deleteOne(collection, OxiDb.obj("k", key));
        return r.optInt("deleted", 0) > 0;
    }

    /** Every stored key. */
    public List<String> keys() {
        JSONArray docs = db.find(collection, OxiDb.obj());
        List<String> out = new ArrayList<>();
        for (int i = 0; i < docs.length(); i++) {
            String k = docs.optJSONObject(i) != null ? docs.optJSONObject(i).optString("k", null) : null;
            if (k != null) {
                out.add(k);
            }
        }
        return out;
    }

    /** Remove every key in this namespace. */
    public void clear() {
        db.delete(collection, OxiDb.obj());
    }
}
