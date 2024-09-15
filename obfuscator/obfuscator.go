package obfuscator

import (
	"strings"
)

func ObfuscateSSN(ssn string) string {
	parts := strings.Split(ssn, "-")
	if len(parts) != 3 {
		return "XXX-XX-" + ssn[len(ssn)-4:]
	}
	return "XXX-XX-" + parts[2]
}

func ObfuscateCreditCard(number string) string {
	if len(number) < 4 {
		return strings.Repeat("X", len(number))
	}
	return strings.Repeat("X", len(number)-4) + number[len(number)-4:]
}
