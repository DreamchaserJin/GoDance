module gdindex

go 1.18

replace (
	search => ../search
	utils => ../utils
)

require (
	github.com/blevesearch/vellum v1.0.7
	github.com/boltdb/bolt v1.3.1
	search v0.0.0
	utils v0.0.0
)

require (
	github.com/adamzy/cedar-go v0.0.0-20170805034717-80a9c64b256d // indirect
	github.com/apsdehal/go-logger v0.0.0-20190515212710-b0d6ccfee0e6 // indirect
	github.com/bits-and-blooms/bitset v1.2.0 // indirect
	github.com/blevesearch/mmap-go v1.0.3 // indirect
	github.com/go-ego/gse v0.70.2 // indirect
	github.com/huichen/sego v0.0.0-20210824061530-c87651ea5c76 // indirect
	github.com/vcaesar/cedar v0.20.1 // indirect
	github.com/yanyiwu/gojieba v1.1.2 // indirect
	golang.org/x/sys v0.0.0-20220520151302-bc2c85ada10a // indirect
)
