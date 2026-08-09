module replication-go

go 1.21

replace github.com/parisxmas/OxiDB/clients/go/oxidb => ../../clients/go/oxidb

replace github.com/parisxmas/OxiDB/clients/go/oxiwire => ../../clients/go/oxiwire

require (
	github.com/parisxmas/OxiDB/clients/go/oxidb v0.0.0-20260329201048-a7f2680e08aa // indirect
	github.com/parisxmas/OxiDB/clients/go/oxiwire v0.0.0 // indirect
)
