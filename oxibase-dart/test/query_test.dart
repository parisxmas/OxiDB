import 'package:oxibase/oxibase.dart';
import 'package:test/test.dart';

void main() {
  group('URL building', () {
    // The URL *is* the query on a PostgREST surface, so building it correctly is
    // most of what this client does. No server involved.
    OxibaseClient client({String? ref, bool tenantInPath = false}) => OxibaseClient(
          url: 'https://api.test/',
          key: 'anon-key',
          ref: ref,
          tenantInPath: tenantInPath,
        );

    test('project goes in the query string by default', () {
      final uri = client(ref: 'proj').from('posts').select().uri;
      expect(uri.path, '/rest/v1/posts');
      expect(uri.queryParameters['db'], 'proj');
      expect(uri.queryParameters['select'], '*');
    });

    test('tenantInPath addresses the project by path instead', () {
      final uri = client(ref: 'proj', tenantInPath: true).from('posts').select().uri;
      expect(uri.path, '/proj/rest/v1/posts');
      expect(uri.queryParameters.containsKey('db'), isFalse);
    });

    test('a trailing slash on the base URL does not double up', () {
      expect(client().from('posts').uri.toString(), startsWith('https://api.test/rest/v1/posts'));
    });

    test('filters, order, limit and offset', () {
      final uri = client()
          .from('posts')
          .select('ts,body')
          .eq('handle', 'ada')
          .gt('ts', 1700000000)
          .isNull('reply_to')
          .order('ts', ascending: false)
          .range(20, 39)
          .uri;
      final q = uri.queryParameters;
      expect(q['select'], 'ts,body');
      expect(q['handle'], 'eq.ada');
      expect(q['ts'], 'gt.1700000000');
      expect(q['reply_to'], 'is.null');
      expect(q['order'], 'ts.desc');
      expect(q['offset'], '20');
      expect(q['limit'], '20');
    });

    test('a numeric-looking string stays a string', () {
      // eq."42" is a text comparison; eq.42 is a number. Getting this wrong makes
      // a query silently miss rows on a schemaless collection.
      expect(client().from('t').eq('code', '42').uri.queryParameters['code'], 'eq."42"');
      expect(client().from('t').eq('code', 42).uri.queryParameters['code'], 'eq.42');
      expect(client().from('t').eq('flag', 'true').uri.queryParameters['flag'], 'eq."true"');
      expect(client().from('t').eq('flag', true).uri.queryParameters['flag'], 'eq.true');
    });

    test('in and contains render as PostgREST expects', () {
      expect(client().from('t').isIn('id', [1, 2, 3]).uri.queryParameters['id'], 'in.(1,2,3)');
      expect(client().from('t').contains('tags', ['rust', 'sql']).uri.queryParameters['tags'],
          'cs.{rust,sql}');
    });

    test('a measurement is selected by profile, not by name', () {
      // A series exists only after its first write, so name-based dispatch could
      // not work; the profile header is what routes it.
      final q = client(ref: 'proj').series('cpu');
      expect(q.uri.path, '/rest/v1/cpu');
    });

    test('update and delete refuse to run without a filter', () async {
      // A forgotten .eq() would otherwise rewrite every row the rules allow.
      await expectLater(
        client().from('posts').update({'body': 'x'}),
        throwsA(isA<OxibaseException>().having((e) => e.message, 'message', contains('every row'))),
      );
      await expectLater(
        client().from('posts').delete(),
        throwsA(isA<OxibaseException>()),
      );
    });
  });

  group('errors carry the distinction the server made', () {
    test('429 is not the same as 403, and 409 is neither', () {
      const rateLimited = OxibaseException('slow down', statusCode: 429, retryAfter: 30);
      const denied = OxibaseException('access denied', statusCode: 403);
      const conflict = OxibaseException('duplicate', statusCode: 409);

      expect(rateLimited.isRateLimited, isTrue);
      expect(rateLimited.isDenied, isFalse, reason: '"not yet" is not "no"');
      expect(rateLimited.retryAfter, 30);
      expect(denied.isDenied, isTrue);
      expect(conflict.isConflict, isTrue);
      expect(conflict.isDenied, isFalse);
    });
  });
}
