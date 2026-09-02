package faker

import (
	_ "embed"
	"encoding/json"
	"hash/fnv"
	"html/template"
	"math/rand"
	"strconv"
	"strings"
)

type fakerData struct {
	FirstNames       []string `json:"firstNames"`
	LastNames        []string `json:"lastNames"`
	StreetNames      []string `json:"streetNames"`
	Cities           []string `json:"cityNames"`
	Companies        []string `json:"companies"`
	CompanySuffixes  []string `json:"companySuffixes"`
	BusinessIdFormat string   `json:"businessIdFormat"`
}

//go:embed fi.json
var fiData []byte

//go:embed se.json
var seData []byte

//go:embed no.json
var noData []byte

//go:embed dk.json
var dkData []byte

//go:embed default.json
var defaultData []byte

func init() {
	loadData("fi", fiData)
	loadData("se", seData)
	loadData("no", noData)
	loadData("dk", dkData)
	loadData("default", defaultData)
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

func getDefaultLocale() string {
	return "default"
}

// localeData resolves the data set for locale, falling back to the default one.
// Faker is also constructed as a plain struct literal, which skips the check in
// NewWithLocale, and an unknown locale would otherwise index into an empty data
// set and panic.
func localeData(locale string) fakerData {
	if data, ok := dataMap[locale]; ok {
		return data
	}
	return dataMap[getDefaultLocale()]
}

func NewWithLocale(locale string) *Faker {
	if _, ok := dataMap[locale]; !ok {
		locale = getDefaultLocale()
	}
	return &Faker{
		Locale: locale,
	}
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
		"transformBusinessId": func(input string) string {
			return TransformBusinessId(input, f.Locale)
		},
	}
}

func TransformFirstName(input string, locale string) string {
	if input == "" {
		return ""
	}
	rng := initRng(input)
	data := localeData(locale)
	return data.FirstNames[rng.Intn(len(data.FirstNames))]
}

func TransformLastName(input string, locale string) string {
	if input == "" {
		return ""
	}
	rng := initRng(input)
	data := localeData(locale)
	return data.LastNames[rng.Intn(len(data.LastNames))]
}

func TransformStreet(input string, locale string) string {
	if input == "" {
		return ""
	}
	rng := initRng(input)
	streetNumner := rng.Intn(1000)
	data := localeData(locale)
	return data.StreetNames[rng.Intn(len(data.StreetNames))] + " " + strconv.Itoa(streetNumner)
}

func TransformCity(input string, locale string) string {
	if input == "" {
		return ""
	}
	rng := initRng(input)
	data := localeData(locale)
	return data.Cities[rng.Intn(len(data.Cities))]
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
	data := localeData(locale)
	companyNameParts := []string{}
	companyNameParts = append(companyNameParts, data.Companies[rng.Intn(len(data.Companies))])
	// add second part?
	if rng.Float32() < 0.5 {
		companyNameParts = append(companyNameParts, data.Companies[rng.Intn(len(data.Companies))])
	}
	// add suffix?
	if rng.Float32() < 0.7 {
		companyNameParts = append(companyNameParts, data.CompanySuffixes[rng.Intn(len(data.CompanySuffixes))])
	}
	return strings.Join(companyNameParts, " ")
}

func TransformBusinessId(input string, locale string) string {
	if input == "" {
		return ""
	}
	rng := initRng(input)

	format := localeData(locale).BusinessIdFormat

	var result strings.Builder
	for _, char := range format {
		if char == '0' {
			result.WriteString(strconv.Itoa(rng.Intn(10)))
		} else {
			result.WriteRune(char)
		}
	}

	return result.String()
}

func initRng(input string) *rand.Rand {
	h := fnv.New64a()
	h.Write([]byte(input))
	return rand.New(rand.NewSource(int64(h.Sum64())))
}
