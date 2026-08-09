module udp-logging-go

go 1.21

require github.com/parisxmas/OxiDB/clients/go/oxidb v0.0.0

require github.com/parisxmas/OxiDB/clients/go/oxiwire v0.0.0 // indirect

replace github.com/parisxmas/OxiDB/clients/go/oxidb => ../../clients/go/oxidb

replace github.com/parisxmas/OxiDB/clients/go/oxiwire => ../../clients/go/oxiwire
