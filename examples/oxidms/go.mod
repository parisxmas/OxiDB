module github.com/parisxmas/OxiDB/OxiDMS

go 1.21

require (
	github.com/go-chi/chi/v5 v5.2.1
	github.com/golang-jwt/jwt/v5 v5.2.1
	github.com/google/uuid v1.6.0
	github.com/parisxmas/OxiDB/go/oxidb v0.0.0
	golang.org/x/crypto v0.31.0
)

replace github.com/parisxmas/OxiDB/go/oxidb => ../go/oxidb
