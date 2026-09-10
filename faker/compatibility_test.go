package faker

import (
	"crypto/sha256"
	"fmt"
	"strconv"
	"testing"
)

var transforms = []struct {
	name string
	fn   func(string, string) string
}{
	{"FirstName", TransformFirstName},
	{"LastName", TransformLastName},
	{"Street", TransformStreet},
	{"City", TransformCity},
	{"FullName", TransformFullName},
	{"CompanyName", TransformCompanyName},
	{"BusinessId", TransformBusinessId},
}

// The fingerprints capture the public outputs before RNG pooling. Mixed calls
// exercise different numbers of random draws, including full names that seed
// first and last names independently. Parallel locales also exercise pool reuse.
func TestTransformCompatibility(t *testing.T) {
	golden := map[string]string{
		"default": "e5b63e857f67a6072ee228751abb4a74985d7c4175e7b2d8e1bb2decb62f8822",
		"fi":      "9ee113ce71fa527ea36b598dd419bbab18682152da3a190f477e273386511897",
		"se":      "3f0286a81d279250926e60756d4acd4eeddbef66026e3bb3d91f85a5ccbeab74",
		"no":      "53b1e1952422ac1f067c2c7bb59ba3ac8b1d582a5dc188a9d51d4d43f2478e6c",
		"dk":      "4a28e77f8ddf2e1e37c034a8678ae83ece2f2872683e32ec31b9d91bc408ddc4",
		"unknown": "e5b63e857f67a6072ee228751abb4a74985d7c4175e7b2d8e1bb2decb62f8822",
	}
	for locale, want := range golden {
		t.Run(locale, func(t *testing.T) {
			t.Parallel()
			h := sha256.New()
			inputs := []string{"", "Alice", "Alice Smith", " Alice Smith ", "Åsa Østergård", "東京 太郎", "\x00\\'\""}
			for i := 0; i < 128; i++ {
				inputs = append(inputs, "person-"+strconv.Itoa(i)+" family-"+strconv.Itoa(i*17))
			}
			for _, input := range inputs {
				for _, transform := range transforms {
					value := transform.fn(input, locale)
					fmt.Fprintf(h, "%d:%s", len(value), value)
				}
			}
			got := fmt.Sprintf("%x", h.Sum(nil))
			if got != want {
				t.Fatalf("deterministic output changed: got fingerprint %s, want %s", got, want)
			}
		})
	}
}

func BenchmarkTransforms(b *testing.B) {
	for _, transform := range transforms {
		b.Run(transform.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				transform.fn("Alice Smith", "no")
			}
		})
	}
}
