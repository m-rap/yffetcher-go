package main

import (
	"fmt"

	"github.com/m-rap/itemprice-go"
	yffetcher "github.com/m-rap/yffetch-go"
)

func main() {
	fmt.Println("vim-go")
	searchItems := `[
	{"id":"AMRT","name":"AMRT",},
	{"id":"ARNA","name":"ARNA",},
	{"id":"BBCA","name":"BBCA",},
	{"id":"EKAD","name":"EKAD",},
	{"id":"GHON","name":"GHON",},
	{"id":"GOTO","name":"GOTO",},
	{"id":"INDF","name":"INDF",},
	{"id":"INDS","name":"INDS",},
	{"id":"INDY","name":"INDY",},
	{"id":"MPMX","name":"MPMX",},
	{"id":"OMED","name":"OMED",},
	{"id":"ULTJ","name":"ULTJ",},
	{"id":"XIHD","name":"XIHD",},
	{"id":"WSKT","name":"WSKT",},
	{"id":"MBMA","name":"MBMA",},
]`
	err := yffetcher.FetchFromJson("itemprice.db", []byte(searchItems), yffetcher.Options{
		ForcePeriod: itemprice.Period1y,
	})
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}
}
