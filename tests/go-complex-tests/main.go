package main

import (
	"fmt"
	"os"
)

var testRegistry = map[string]func(){
	"mixed_workload": testMixedWorkload,
	"pagination":     testPagination,
	"nested_query":   testNestedQuery,
	"pipeline":       testPipeline,
	"lookup":         testLookup,
	"large_result":   testLargeResult,
	"hot_key":        testHotKey,
	"write_heavy":    testWriteHeavy,
	"bulk_ops":       testBulkOps,
	"scale_test":     testScaleTest,
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: bench-complex <test_name|all>")
		os.Exit(1)
	}

	testName := os.Args[1]

	if testName == "all" {
		order := []string{
			"mixed_workload", "pagination", "nested_query", "pipeline",
			"lookup", "large_result", "hot_key", "write_heavy", "bulk_ops", "scale_test",
		}
		for _, name := range order {
			fmt.Printf("\n╔══════════════════════════════════════════════════════════════╗\n")
			fmt.Printf("║   Test: %-51s ║\n", name)
			fmt.Printf("╚══════════════════════════════════════════════════════════════╝\n\n")
			results = nil
			testRegistry[name]()
		}
		return
	}

	fn, ok := testRegistry[testName]
	if !ok {
		fmt.Printf("Unknown test: %s\n", testName)
		fmt.Println("Available: mixed_workload, pagination, nested_query, pipeline, lookup, large_result, hot_key, write_heavy, bulk_ops, scale_test")
		os.Exit(1)
	}

	fmt.Printf("╔══════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║   Test: %-51s ║\n", testName)
	fmt.Printf("╚══════════════════════════════════════════════════════════════╝\n\n")

	fn()
}
