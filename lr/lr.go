package lr

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"

	"github.com/pkg/errors"
)

func Ingest(f io.Reader) (
	header []string,
	data [][]string,
	indices []map[string][]int,
	err error,
) {
	r := csv.NewReader(f)

	if header, err = r.Read(); err != nil {
		return
	}

	indices = make([]map[string][]int, len(header))
	var rowCount, colCount int = 0, len(header)

	for rec, err := r.Read(); err == nil; rec, err = r.Read() {

		if len(rec) != colCount {
			return nil, nil, nil, errors.Errorf(
				"Expected cols %d. Got %d cols in row %d",
				colCount, len(rec), rowCount)
		}

		data = append(data, rec)

		for j, val := range rec {
			if indices[j] == nil {
				indices[j] = make(map[string][]int)
			}
			indices[j][val] = append(indices[j][val], rowCount)
		}

		rowCount++
	}

	return
}

func Cardinality(indices []map[string][]int) (retVal []int) {

	retVal = make([]int, len(indices))

	for i, m := range indices {
		retVal[i] = len(m)
	}

	return
}

func PrintColumn(source string, colIdx int) error {

	f, err := os.Open(source)

	if err != nil {
		return err
	}

	r := csv.NewReader(f)

	hdr, err := r.Read()

	if err != nil {
		return err
	}

	fmt.Println("")
	fmt.Printf("%s\n========\n", hdr[colIdx])

	for rec, err := r.Read(); err == nil; rec, err = r.Read() {

		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		fmt.Printf("%s\n", rec[colIdx])
	}

	return nil
}
