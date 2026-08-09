module benchmark-1m

go 1.21

require (
	github.com/parisxmas/OxiDB/clients/go/oxidb v0.0.0
	go.mongodb.org/mongo-driver v1.17.3
)

require (
	github.com/golang/snappy v0.0.4 // indirect
	github.com/klauspost/compress v1.16.7 // indirect
	github.com/montanaflynn/stats v0.7.1 // indirect
	github.com/parisxmas/OxiDB/clients/go/oxiwire v0.0.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	golang.org/x/crypto v0.26.0 // indirect
	golang.org/x/sync v0.8.0 // indirect
	golang.org/x/text v0.17.0 // indirect
)

replace github.com/parisxmas/OxiDB/clients/go/oxidb => ../../clients/go/oxidb

replace github.com/parisxmas/OxiDB/clients/go/oxiwire => ../../clients/go/oxiwire
