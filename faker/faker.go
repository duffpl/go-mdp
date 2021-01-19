package faker

import (
	"crypto/sha256"
	_ "embed"
	"encoding/binary"
	"encoding/json"
	"html/template"
	"math/rand"
	"strconv"
	"strings"
)

type fakerData struct {
	FirstNames      []string `json:"firstNames"`
	LastNames       []string `json:"lastNames"`
	StreetNames     []string `json:"streetNames"`
	Cities          []string `json:"cityNames"`
	Companies       []string `json:"companies"`
	CompanySuffixes []string `json:"companySuffixes"`
}

//go:embed fi.json
var fiData []byte

//go:embed se.json
var seData []byte

//go:embed no.json
var noData []byte

func init() {
	loadData("fi", fiData)
	loadData("se", seData)
	loadData("no", noData)
}

var dataMap = make(map[string]fakerData)

func loadData(locale string, data []byte) {
	var fd fakerData
	err := json.Unmarshal(data, &fd)
	if err != nil {
		panic(err)
	}
	dataMap[locale] = fd
}

type Faker struct {
	Locale string
}

func (f *Faker) FuncMap() template.FuncMap {
	return template.FuncMap{
		"transformFirstName": func(input string) string {
			return TransformFirstName(input, f.Locale)
		},
		"transformLastName": func(input string) string {
			return TransformLastName(input, f.Locale)
		},
		"transformStreet": func(input string) string {
			return TransformStreet(input, f.Locale)
		},
		"transformCity": func(input string) string {
			return TransformCity(input, f.Locale)
		},
		"transformCompanyName": func(input string) string {
			return TransformCompanyName(input, f.Locale)
		},
		"transformFullName": func(input string) string {
			return TransformFullName(input, f.Locale)
		},
	}
}

func TransformFirstName(input string, locale string) string {
	if input == "" {
		return ""
	}
	rng := initRng(input)
	return dataMap[locale].FirstNames[rng.Intn(len(dataMap[locale].FirstNames))]
}

func TransformLastName(input string, locale string) string {
	if input == "" {
		return ""
	}
	rng := initRng(input)
	return dataMap[locale].LastNames[rng.Intn(len(dataMap[locale].LastNames))]
}

func TransformStreet(input string, locale string) string {
	if input == "" {
		return ""
	}
	rng := initRng(input)
	streetNumner := rng.Intn(1000)
	return dataMap[locale].StreetNames[rng.Intn(len(dataMap[locale].StreetNames))] + " " + strconv.Itoa(streetNumner)
}

func TransformCity(input string, locale string) string {
	if input == "" {
		return ""
	}
	rng := initRng(input)
	return dataMap[locale].Cities[rng.Intn(len(dataMap[locale].Cities))]
}

func TransformFullName(input string, locale string) string {
	if input == "" {
		return ""
	}
	firstName, lastName, _ := strings.Cut(input, " ")
	nameTokens := []string{
		TransformFirstName(firstName, locale),
		TransformLastName(lastName, locale),
	}
	return strings.Join(nameTokens, " ")
}

func TransformCompanyName(input string, locale string) string {
	if input == "" {
		return ""
	}
	rng := initRng(input)
	companyNameParts := []string{}
	companyNameParts = append(companyNameParts, dataMap[locale].Companies[rng.Intn(len(dataMap[locale].Companies))])
	// add second part?
	if rng.Float32() < 0.5 {
		companyNameParts = append(companyNameParts, dataMap[locale].Companies[rng.Intn(len(dataMap[locale].Companies))])
	}
	// add suffix?
	if rng.Float32() < 0.7 {
		companyNameParts = append(companyNameParts, dataMap[locale].CompanySuffixes[rng.Intn(len(dataMap[locale].CompanySuffixes))])
	}
	return strings.Join(companyNameParts, " ")
}

func initRng(input string) *rand.Rand {
	hash := sha256.Sum256([]byte(input))
	seed := binary.BigEndian.Uint64(hash[:8])
	return rand.New(rand.NewSource(int64(seed)))
}
