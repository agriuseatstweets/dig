package main

import (
	"time"
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestRunFrom(t *testing.T) {
	format := "2006-01-02T15:04:05"

	start, _ := time.Parse(format, "2020-01-01T15:04:05")
	res := makeRange(start, start)
	expected := []string{"2020-01-01"}

	assert.Equal(t, res, expected)

	end, _ := time.Parse(format, "2020-01-04T15:04:05")
	res = makeRange(start, end)
	expected = []string{"2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04"}
	assert.Equal(t, res, expected)


	start, _ = time.Parse("2006-01-02", "2020-01-01")
	end, _ = time.Parse("2006-01-02", "2020-01-04")
	res = makeRange(start, end)
	expected = []string{"2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04"}
	assert.Equal(t, res, expected)
}
