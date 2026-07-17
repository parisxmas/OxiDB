using Google.Apis.Auth;

namespace BugTracker;

/// The signed-in caller, as proved by a Google ID token.
public record Principal(string Sub, string Email, string Name, string? Picture, bool IsAdmin);

/// Validates Google ID tokens and decides who is an admin. This is the ENTIRE
/// authentication story: the browser does "Sign in with Google" (Google
/// Identity Services), gets an ID token, and sends it as a Bearer header. Here
/// we verify that token against Google's keys with Google's own library — no
/// passwords, no sessions, no account table of our own.
public sealed class GoogleAuth
{
    private readonly string _clientId;
    private readonly HashSet<string> _admins;

    public GoogleAuth(IConfiguration cfg)
    {
        // `??` only falls through on null, but appsettings ships an EMPTY
        // "Google:ClientId" — which would shadow the env var and silently
        // disable auth. Treat blank as "not set".
        static string? NonEmpty(string? s) => string.IsNullOrWhiteSpace(s) ? null : s;

        // The OAuth 2.0 Web client id from Google Cloud Console. It is the
        // `aud` we require on every token, so a token minted for some other
        // site cannot be replayed against this API. Env wins over appsettings.
        _clientId = NonEmpty(Environment.GetEnvironmentVariable("BUGS_GOOGLE_CLIENT_ID"))
                    ?? NonEmpty(cfg["Google:ClientId"])
                    ?? "";

        var admins = NonEmpty(Environment.GetEnvironmentVariable("BUGS_ADMIN_EMAILS"))
                     ?? NonEmpty(cfg["Admins"])
                     ?? "barisakin@gmail.com";
        _admins = admins.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                        .Select(e => e.ToLowerInvariant())
                        .ToHashSet();
    }

    public bool Configured => _clientId.Length > 0;

    /// Pull "Authorization: Bearer &lt;idToken&gt;" and validate it. Returns the
    /// caller, or null if there is no token / it does not check out.
    public async Task<Principal?> AuthenticateAsync(HttpRequest req)
    {
        if (!Configured) return null;

        var header = req.Headers.Authorization.ToString();
        if (string.IsNullOrWhiteSpace(header) ||
            !header.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
            return null;

        var token = header["Bearer ".Length..].Trim();
        if (token.Length == 0) return null;

        GoogleJsonWebSignature.Payload payload;
        try
        {
            payload = await GoogleJsonWebSignature.ValidateAsync(token,
                new GoogleJsonWebSignature.ValidationSettings { Audience = new[] { _clientId } });
        }
        catch (InvalidJwtException)
        {
            return null; // expired, wrong audience, bad signature — all "not signed in"
        }

        // A Google account without a verified email is not one we let file bugs
        // under an identity — it would let anyone claim any address.
        if (payload.EmailVerified != true || string.IsNullOrEmpty(payload.Email))
            return null;

        var email = payload.Email;
        return new Principal(
            Sub: payload.Subject,
            Email: email,
            Name: string.IsNullOrWhiteSpace(payload.Name) ? email : payload.Name,
            Picture: payload.Picture,
            IsAdmin: _admins.Contains(email.ToLowerInvariant()));
    }
}
