"use strict";
/**
 * Parse MongoDB-style shell syntax into OxiDB JSON commands.
 *
 * Supported patterns:
 *   db.collection.find({query}, {sort}, limit)
 *   db.collection.findOne({query})
 *   db.collection.insert({doc})
 *   db.collection.insertMany([{doc1}, {doc2}])
 *   db.collection.update({query}, {update})
 *   db.collection.delete({query})
 *   db.collection.count({query})
 *   db.collection.aggregate([{pipeline}])
 *   db.collection.createIndex("field")
 *   db.collection.drop()
 *   collection.find({...})       (without db. prefix)
 *   show collections
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.parseShellQuery = parseShellQuery;
function parseShellQuery(input) {
    const trimmed = input.trim();
    if (!trimmed) {
        return null;
    }
    // Already JSON command — pass through
    if (trimmed.startsWith('{') && trimmed.includes('"cmd"')) {
        try {
            return JSON.parse(trimmed);
        }
        catch { /* fall through */ }
    }
    // show collections
    if (/^show\s+(collections|tables)$/i.test(trimmed)) {
        return { cmd: 'list_collections' };
    }
    // show indexes <collection>
    const showIdx = trimmed.match(/^show\s+indexes?\s+(\w+)$/i);
    if (showIdx) {
        return { cmd: 'list_indexes', collection: showIdx[1] };
    }
    // db.collection.method(...) or collection.method(...)
    const match = trimmed.match(/^(?:db\.)?(\w+)\.(find|findOne|find_one|insert|insertMany|insert_many|update|updateOne|update_one|delete|deleteOne|delete_one|deleteMany|count|aggregate|createIndex|create_index|drop|listIndexes|list_indexes)\s*\(([\s\S]*)\)$/);
    if (!match) {
        return null;
    }
    const collection = match[1];
    const method = match[2];
    const argsStr = match[3].trim();
    // Parse arguments — split by top-level commas
    const args = parseArgs(argsStr);
    switch (method) {
        case 'find': {
            const cmd = { cmd: 'find', collection, query: parseJsonArg(args[0]) || {} };
            if (args[1]) {
                const sortOrLimit = parseJsonArg(args[1]);
                if (typeof sortOrLimit === 'number') {
                    cmd.limit = sortOrLimit;
                }
                else if (sortOrLimit && typeof sortOrLimit === 'object') {
                    cmd.sort = sortOrLimit;
                }
            }
            if (args[2]) {
                const limit = parseInt(args[2]);
                if (!isNaN(limit)) {
                    cmd.limit = limit;
                }
            }
            if (!cmd.limit) {
                cmd.limit = 50;
            }
            return cmd;
        }
        case 'findOne':
        case 'find_one':
            return { cmd: 'find_one', collection, query: parseJsonArg(args[0]) || {} };
        case 'insert':
            return { cmd: 'insert', collection, doc: parseJsonArg(args[0]) || {} };
        case 'insertMany':
        case 'insert_many':
            return { cmd: 'insert_many', collection, docs: parseJsonArg(args[0]) || [] };
        case 'update':
        case 'updateOne':
        case 'update_one':
            return { cmd: 'update', collection, query: parseJsonArg(args[0]) || {}, update: parseJsonArg(args[1]) || {} };
        case 'delete':
        case 'deleteOne':
        case 'delete_one':
            return { cmd: 'delete_one', collection, query: parseJsonArg(args[0]) || {} };
        case 'deleteMany':
            return { cmd: 'delete', collection, query: parseJsonArg(args[0]) || {} };
        case 'count':
            return { cmd: 'count', collection, query: parseJsonArg(args[0]) || {} };
        case 'aggregate':
            return { cmd: 'aggregate', collection, pipeline: parseJsonArg(args[0]) || [] };
        case 'createIndex':
        case 'create_index': {
            const field = args[0]?.replace(/['"]/g, '').trim();
            return { cmd: 'create_index', collection, field };
        }
        case 'drop':
            return { cmd: 'drop_collection', collection };
        case 'listIndexes':
        case 'list_indexes':
            return { cmd: 'list_indexes', collection };
        default:
            return null;
    }
}
/** Parse a JSON-like argument, handling relaxed syntax (unquoted keys) */
function parseJsonArg(arg) {
    if (!arg || !arg.trim()) {
        return undefined;
    }
    let s = arg.trim();
    // Try standard JSON first
    try {
        return JSON.parse(s);
    }
    catch { /* continue */ }
    // Relaxed JSON: add quotes to unquoted keys
    // {name: "Alice"} → {"name": "Alice"}
    s = s.replace(/(\{|,)\s*([a-zA-Z_$][a-zA-Z0-9_$]*)\s*:/g, '$1"$2":');
    // Handle $operators: $gt → "$gt"
    s = s.replace(/(\{|,)\s*(\$[a-zA-Z_]+)\s*:/g, '$1"$2":');
    try {
        return JSON.parse(s);
    }
    catch { /* continue */ }
    // Maybe it's a plain number
    const n = Number(s);
    if (!isNaN(n)) {
        return n;
    }
    // Maybe it's a plain string
    if ((s.startsWith("'") && s.endsWith("'")) || (s.startsWith('"') && s.endsWith('"'))) {
        return s.slice(1, -1);
    }
    return s;
}
/** Split arguments by top-level commas (respecting nesting) */
function parseArgs(str) {
    if (!str.trim()) {
        return [];
    }
    const args = [];
    let depth = 0;
    let current = '';
    for (const ch of str) {
        if (ch === '{' || ch === '[' || ch === '(') {
            depth++;
        }
        if (ch === '}' || ch === ']' || ch === ')') {
            depth--;
        }
        if (ch === ',' && depth === 0) {
            args.push(current.trim());
            current = '';
        }
        else {
            current += ch;
        }
    }
    if (current.trim()) {
        args.push(current.trim());
    }
    return args;
}
//# sourceMappingURL=queryParser.js.map