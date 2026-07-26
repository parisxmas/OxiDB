import 'dart:convert';

import 'transport.dart';
import 'types.dart';

/// A query against one collection or table, built up as a URL.
///
/// The server implements PostgREST, so a filter is a query parameter and nothing
/// here has to know which engine answers: a name that is a SQL table is served by
/// the SQL engine, anything else by the document engine. Time-series is the one
/// exception and is selected explicitly — see `OxibaseClient.tsdb`.
///
/// Await the builder to run it: `await client.from('posts').select().eq('handle', 'ada')`.
class OxibaseQuery {
  OxibaseQuery(this._transport, this._table, {Map<String, String>? profile})
      : _profileHeaders = profile ?? const {};

  final Transport _transport;
  final String _table;
  final Map<String, String> _profileHeaders;

  final List<(String, String)> _filters = [];
  final List<String> _order = [];
  String _select = '*';
  int? _limit;
  int? _offset;

  // ── filters ───────────────────────────────────────────────────────────────
  // A value is rendered rather than escaped: PostgREST takes `col=eq.value`, and
  // a string that could be read as a number is quoted so `eq."42"` stays a
  // string. Uri handles the percent-encoding.

  OxibaseQuery select([String columns = '*']) {
    _select = columns;
    return this;
  }

  OxibaseQuery eq(String column, Object? value) => _filter(column, 'eq', value);
  OxibaseQuery neq(String column, Object? value) => _filter(column, 'neq', value);
  OxibaseQuery gt(String column, Object? value) => _filter(column, 'gt', value);
  OxibaseQuery gte(String column, Object? value) => _filter(column, 'gte', value);
  OxibaseQuery lt(String column, Object? value) => _filter(column, 'lt', value);
  OxibaseQuery lte(String column, Object? value) => _filter(column, 'lte', value);

  /// Case-sensitive pattern match; `%` and `_` are the wildcards.
  OxibaseQuery like(String column, String pattern) => _filter(column, 'like', pattern);
  OxibaseQuery ilike(String column, String pattern) => _filter(column, 'ilike', pattern);

  /// `column IS NULL` / `IS TRUE` / `IS FALSE`.
  ///
  /// These bypass the value quoting on purpose: `is.null` is the null *value*,
  /// while `is."null"` would compare against the four-letter string — which the
  /// quoting rule for ambiguous strings would otherwise produce.
  OxibaseQuery isNull(String column) => _raw(column, 'is.null');
  OxibaseQuery isTrue(String column) => _raw(column, 'is.true');
  OxibaseQuery isFalse(String column) => _raw(column, 'is.false');

  OxibaseQuery _raw(String column, String expr) {
    _filters.add((column, expr));
    return this;
  }

  OxibaseQuery isIn(String column, Iterable<Object?> values) {
    _filters.add((column, 'in.(${values.map(_render).join(',')})'));
    return this;
  }

  /// The array contains all of these — `tags=cs.{a,b}`.
  OxibaseQuery contains(String column, Iterable<Object?> values) {
    _filters.add((column, 'cs.{${values.map((v) => v.toString()).join(',')}}'));
    return this;
  }

  /// Negate any of the above: `not('body', 'ilike', '%spam%')`.
  OxibaseQuery not(String column, String operator, Object? value) {
    _filters.add((column, 'not.$operator.${_render(value)}'));
    return this;
  }

  /// Raw `or=(…)`, for the cases the fluent form cannot express.
  OxibaseQuery or(String expression) {
    _filters.add(('or', '($expression)'));
    return this;
  }

  OxibaseQuery order(String column, {bool ascending = true}) {
    _order.add('$column.${ascending ? 'asc' : 'desc'}');
    return this;
  }

  OxibaseQuery limit(int count) {
    _limit = count;
    return this;
  }

  OxibaseQuery range(int from, int to) {
    _offset = from;
    _limit = to - from + 1;
    return this;
  }

  /// Ask for one row. Pairs with [maybeSingle], which returns it or null.
  OxibaseQuery single() {
    _limit ??= 1;
    return this;
  }

  OxibaseQuery _filter(String column, String op, Object? value) {
    _filters.add((column, '$op.${_render(value)}'));
    return this;
  }

  static String _render(Object? value) {
    if (value == null) return 'null';
    if (value is num || value is bool) return value.toString();
    final s = value.toString();
    // Quote a string that would otherwise be read as a number or a keyword, so
    // the server compares it as text.
    final ambiguous = num.tryParse(s) != null || s == 'true' || s == 'false' || s == 'null';
    return ambiguous ? '"$s"' : s;
  }

  Map<String, String> _params() => {
        'select': _select,
        for (final (column, expr) in _filters) column: expr,
        if (_order.isNotEmpty) 'order': _order.join(','),
        if (_limit != null) 'limit': '$_limit',
        if (_offset != null) 'offset': '$_offset',
      };

  /// The URL this query would request — exposed because it makes the builder
  /// testable without a server, and debuggable with one.
  Uri get uri => _transport.url('/rest/v1/$_table', _params());

  // ── running it ────────────────────────────────────────────────────────────

  Future<List<Map<String, dynamic>>> _run() async {
    final res = await _transport.send('GET', uri, headers: _profileHeaders);
    return _transport.decodeListOrThrow(res);
  }

  /// Run the read and return the rows.
  ///
  /// Dart has no thenable adoption, so the builder is not itself awaitable — the
  /// call ends in `.get()` rather than `await`ing the builder as the JavaScript
  /// client does. Being explicit is the better trade than a fake Future.
  Future<List<Map<String, dynamic>>> get() => _run();

  /// How many rows match, without fetching them.
  ///
  /// Uses the native `/api/{collection}/count`, not a PostgREST count: this
  /// server's `Content-Range` reports the page but leaves the total as `*`, so
  /// there is nothing to read a total from. Only equality filters translate,
  /// which is what that endpoint takes.
  Future<int> count() async {
    final equalities = <String, Object?>{};
    for (final (column, expr) in _filters) {
      if (!expr.startsWith('eq.')) {
        throw OxibaseException(
          'count() takes equality filters only; `$column=$expr` cannot be expressed',
        );
      }
      final raw = expr.substring(3);
      equalities[column] = raw.startsWith('"') && raw.endsWith('"')
          ? raw.substring(1, raw.length - 1)
          : (num.tryParse(raw) ??
              (raw == 'true'
                  ? true
                  : raw == 'false'
                      ? false
                      : raw));
    }
    final uri = _transport.url(
      '/api/${Uri.encodeComponent(_table)}/count',
      equalities.isEmpty ? null : {'q': jsonEncode(equalities)},
    );
    final res = await _transport.send('GET', uri, headers: _profileHeaders);
    final body = _transport.decodeOrThrow(res);
    return (body['count'] as num?)?.toInt() ?? 0;
  }

  /// Run and return the first row, or null.
  Future<Map<String, dynamic>?> maybeSingle() async {
    final rows = await single()._run();
    return rows.isEmpty ? null : rows.first;
  }

  /// Insert one row or many. Returns the rows when [returning] is true.
  Future<List<Map<String, dynamic>>> insert(Object payload, {bool returning = false}) async {
    final res = await _transport.send(
      'POST',
      _transport.url('/rest/v1/$_table'),
      body: payload,
      headers: {
        'Content-Type': 'application/json',
        ..._profileHeaders,
        if (returning) 'Prefer': 'return=representation',
      },
    );
    return _transport.decodeListOrThrow(res);
  }

  /// Update the rows this query's filters select.
  Future<List<Map<String, dynamic>>> update(Map<String, dynamic> patch,
      {bool returning = false}) async {
    _requireFilter('update');
    final res = await _transport.send(
      'PATCH',
      _transport.url('/rest/v1/$_table', _paramsWithoutSelect()),
      body: patch,
      headers: {
        'Content-Type': 'application/json',
        ..._profileHeaders,
        if (returning) 'Prefer': 'return=representation',
      },
    );
    return _transport.decodeListOrThrow(res);
  }

  /// Delete the rows this query's filters select.
  Future<List<Map<String, dynamic>>> delete({bool returning = false}) async {
    _requireFilter('delete');
    final res = await _transport.send(
      'DELETE',
      _transport.url('/rest/v1/$_table', _paramsWithoutSelect()),
      headers: {
        ..._profileHeaders,
        if (returning) 'Prefer': 'return=representation',
      },
    );
    return _transport.decodeListOrThrow(res);
  }

  Map<String, String> _paramsWithoutSelect() => {
        for (final (column, expr) in _filters) column: expr,
        if (_limit != null) 'limit': '$_limit',
      };

  /// A filterless update or delete would rewrite the whole collection, and the
  /// rules would happily allow it for rows the caller owns. Refuse locally: it is
  /// almost always a forgotten `.eq()`.
  void _requireFilter(String verb) {
    if (_filters.isEmpty) {
      throw OxibaseException(
        'refusing to $verb every row: add a filter (this is a client-side guard, '
        'not the server refusing)',
      );
    }
  }
}

/// JSON helper used by the storage and search calls.
String encodeJson(Object? value) => jsonEncode(value);
