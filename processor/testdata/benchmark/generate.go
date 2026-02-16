//go:build ignore

package main

import (
	"fmt"
	"os"
	"strings"
)

func main() {
	sizes := map[string]int{
		"small":   100,
		"medium":  1000,
		"large":   10000,
		"xlarge":  100000,  // ~20 MB
		"xxlarge": 500000,  // ~100 MB
		"huge":    2000000, // ~400 MB
	}

	for name, count := range sizes {
		if err := generateBenchmarkFile(name, count); err != nil {
			fmt.Fprintf(os.Stderr, "Error generating %s: %v\n", name, err)
			os.Exit(1)
		}
		fmt.Printf("Generated %s.sql with %d rows\n", name, count)
	}
}

func generateBenchmarkFile(name string, rowCount int) error {
	var sb strings.Builder

	// Write header
	sb.WriteString("-- Benchmark dataset: " + name + "\n")
	sb.WriteString(fmt.Sprintf("-- Contains %d rows of user data\n\n", rowCount))

	// Create table
	sb.WriteString("DROP TABLE IF EXISTS `benchmark_users`;\n")
	sb.WriteString(`CREATE TABLE ` + "`benchmark_users`" + ` (
  ` + "`id`" + ` bigint(20) NOT NULL,
  ` + "`email`" + ` varchar(255) NOT NULL,
  ` + "`first_name`" + ` varchar(100) NOT NULL,
  ` + "`last_name`" + ` varchar(100) NOT NULL,
  ` + "`phone`" + ` varchar(20) DEFAULT NULL,
  ` + "`address`" + ` varchar(500) DEFAULT NULL,
  ` + "`city`" + ` varchar(100) DEFAULT NULL,
  ` + "`country`" + ` varchar(100) DEFAULT NULL,
  ` + "`company`" + ` varchar(255) DEFAULT NULL,
  ` + "`notes`" + ` text,
  ` + "`created_at`" + ` datetime NOT NULL,
  ` + "`updated_at`" + ` datetime DEFAULT NULL,
  PRIMARY KEY (` + "`id`" + `)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`)
	sb.WriteString("\n")

	// Sample data for variety
	firstNames := []string{"John", "Jane", "Michael", "Sarah", "David", "Emma", "James", "Lisa", "Robert", "Anna"}
	lastNames := []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Davis", "Miller", "Wilson", "Moore", "Taylor"}
	cities := []string{"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "Austin"}
	countries := []string{"USA", "Canada", "UK", "Germany", "France", "Spain", "Italy", "Netherlands", "Sweden", "Norway"}
	companies := []string{"Acme Corp", "Globex Inc", "Initech", "Umbrella Corp", "Wayne Enterprises", "Stark Industries", "Cyberdyne", "Oscorp", "LexCorp", "Soylent Corp"}

	// Generate inserts in batches of 100 for efficiency
	batchSize := 100
	for i := 0; i < rowCount; i += batchSize {
		end := i + batchSize
		if end > rowCount {
			end = rowCount
		}

		sb.WriteString("INSERT INTO `benchmark_users` VALUES ")
		for j := i; j < end; j++ {
			if j > i {
				sb.WriteString(",")
			}
			id := j + 1
			firstName := firstNames[j%len(firstNames)]
			lastName := lastNames[j%len(lastNames)]
			email := fmt.Sprintf("%s.%s.%d@example.com", strings.ToLower(firstName), strings.ToLower(lastName), id)
			phone := fmt.Sprintf("+1-%03d-%03d-%04d", (id*7)%1000, (id*13)%1000, (id*17)%10000)
			address := fmt.Sprintf("%d %s Street, Apt %d", (id*23)%9999, lastName, (id*3)%100)
			city := cities[j%len(cities)]
			country := countries[j%len(countries)]
			company := companies[j%len(companies)]
			notes := fmt.Sprintf("Customer since 20%02d. Priority level: %s.", 10+(id%15), []string{"low", "medium", "high"}[id%3])

			sb.WriteString(fmt.Sprintf("\n(%d,'%s','%s','%s','%s','%s','%s','%s','%s','%s','2023-%02d-%02d 10:30:00',NULL)",
				id, email, firstName, lastName, phone, address, city, country, company, notes,
				(id%12)+1, (id%28)+1))
		}
		sb.WriteString(";\n")
	}

	return os.WriteFile(name+".sql", []byte(sb.String()), 0644)
}
