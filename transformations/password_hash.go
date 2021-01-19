package transformations

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/bcrypt"
)

func argon2Hash(password string) (hash string, err error) {
	salt := make([]byte, 16)
	_, err = rand.Read(salt)
	if err != nil {
		return "", err
	}
	key := argon2.Key([]byte(password), salt, 4, 65536, 1, 32)
	b64Salt := base64.RawStdEncoding.EncodeToString(salt)
	b64Key := base64.RawStdEncoding.EncodeToString(key)
	return fmt.Sprintf("$argon2i$v=19$m=65536,t=4,p=1$%s$%s", b64Salt, b64Key), nil
}

func bcryptHash(password string) (string, error) {
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hashedPassword), nil
}
