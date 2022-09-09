package ecies

import (
	"crypto/aes"
	"fmt"
	"testing"
)

func TestAES(t *testing.T) {
	key := "12345678901234567890123456789012"
	iv := "1234567890123456"
	plaintext := "abcdefghijklmnopqrstuvwxyzABCDEF"
	fmt.Println("Data to encode: ", plaintext)
	encrypt, err := Ase256(plaintext, key, iv, aes.BlockSize)
	if err != nil {
		return
	}
	cipherText := fmt.Sprintf("%v", encrypt)
	fmt.Println("Encode Result:\t", cipherText)
	fmt.Println("Decode Result:\t", Ase256Decode(cipherText, key, iv))

}

func TestPublic(t *testing.T) {
	const PRIVATE = "3621626f8ed1fd98bbd27b9a6af0f8fe3377a6dd66bfa269004da8abfb972fcd"
	var pubKey = Public(PRIVATE)
	fmt.Println("publicKey: \t", pubKey)

	// https://github.com/wsddn/go-ecdh/blob/48726bab92085232373de4ec5c51ce7b441c63a0/elliptic.go#L10
	// crypto.PrivateKey

	// elliptic.Curve.ScalarMult()

	// prv2 = ecies.ImportECDSA(PRIVATE)
	// b := []byte(PRIVATE)
	// var prvKey = ToECDSA(b)
	// fmt.Println("prvKey: \t", p)
}
