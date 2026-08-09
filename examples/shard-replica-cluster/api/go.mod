module shopedge/api

go 1.22

require github.com/parisxmas/OxiDB/clients/go/oxidb v0.0.0-00010101000000-000000000000

require github.com/parisxmas/OxiDB/clients/go/oxiwire v0.0.0 // indirect

replace github.com/parisxmas/OxiDB/clients/go/oxidb => ../../../clients/go/oxidb

replace github.com/parisxmas/OxiDB/clients/go/oxiwire => ../../../clients/go/oxiwire
