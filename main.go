package main

import (
	"fmt"
	"os"
	"strconv"

	"yourtechy.com/go_ml/lr"
)

func main() {

	// default command
	if 1 == len(os.Args) {

		fmt.Println("Error: Invalid number of parameters.")
		printUsage()
	}

	args := os.Args[1:]

	if "?" == args[0] || "help" == args[0] {

		printUsage()

	} else if "--cols" == args[0] {

		if 3 == len(args) {

			idx, err := strconv.Atoi(args[2])
			if err != nil {
				panic(err)
			}

			fmt.Printf("printDataColumns, source=%s, index=%d\n", args[1], idx)

			err = printDataColumns(args[1], idx)

			if err != nil {
				panic(err)
			}

		} else {

			fmt.Println("Error: Invalid number of parameters.")
			printUsage()

		}

	} else if "--card" == args[0] {

		if 2 == len(args) {

			getCardinality(args[1])

		} else {

			fmt.Println("Error: Invalid number of parameters.")
			printUsage()

		}
	} else {

		fmt.Println("Error: Unknown or missing arguments in command")
		printUsage()

	}
}

func printUsage() {
	fmt.Println("Usage: go_ml [Args...]")
	fmt.Println("Args:")
	fmt.Println("'?, help' - show this help message")
	fmt.Println("'--cols source_file index' - print column header and value of column index of source csv")
	fmt.Println("'--card source_file' - execute cardinality of each column in source csv")
}

func printDataColumns(source string, index int) error {
	return lr.PrintColumn(source, index)
}

func getCardinality(source string) {

	f, err := os.Open(source)

	if err != nil {
		panic(err)
	}

	hdr, data, indices, err := lr.Ingest(f)

	if err != nil {
		panic(err)
	}

	c := lr.Cardinality(indices)

	fmt.Printf("Original Data: \nRows: %d, cols: %d\n========\n",
		len(data), len(hdr))

	c = lr.Cardinality(indices)

	for i, h := range hdr {
		fmt.Printf("[%d] %v: %v\n", i, h, c[i])
	}

	fmt.Println("")
}
