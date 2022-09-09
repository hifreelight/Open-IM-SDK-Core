package ecies

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/crypto/secp256k1"
)

func main() {
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

// func AES256CbcEncrypt(iv []byte, key []byte, plaintext []byte) (ciphertext []byte, err error) {
// }

func Ase256(plaintext string, key string, iv string, blockSize int) (string, error) {
	bKey := []byte(key)
	bIV := []byte(iv)
	bPlaintext := PKCS5Padding([]byte(plaintext), blockSize, len(plaintext))
	block, err := aes.NewCipher(bKey)
	if err != nil {
		return "", err
	}
	ciphertext := make([]byte, len(bPlaintext))
	mode := cipher.NewCBCEncrypter(block, bIV)
	mode.CryptBlocks(ciphertext, bPlaintext)
	return hex.EncodeToString(ciphertext), nil
}
func PKCS5UnPadding(src []byte) []byte {
	length := len(src)
	unpadding := int(src[length-1])
	return src[:(length - unpadding)]
}
func PKCS5Padding(ciphertext []byte, blockSize int, after int) []byte {
	padding := (blockSize - len(ciphertext)%blockSize)
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}
func Ase256Decode(cipherText string, encKey string, iv string) (decryptedString string) {
	bKey := []byte(encKey)
	bIV := []byte(iv)
	cipherTextDecoded, err := hex.DecodeString(cipherText)
	if err != nil {
		panic(err)
	}
	block, err := aes.NewCipher(bKey)
	if err != nil {
		panic(err)
	}
	mode := cipher.NewCBCDecrypter(block, bIV)
	mode.CryptBlocks([]byte(cipherTextDecoded), []byte(cipherTextDecoded))
	return string(PKCS5UnPadding(cipherTextDecoded))
}

func EciesEncrypt(pubKeyTo []byte, plaintext []byte, privateKeyFrom []byte) {

}

func EciesDecrypt(privKey []byte, encrypted []byte, publicKeyFrom []byte) {

}

/**
 * return whith 0x04
 */
func Public(privateKey string) (publicKey string) {
	var e ecdsa.PrivateKey
	e.D, _ = new(big.Int).SetString(privateKey, 16)
	e.PublicKey.Curve = secp256k1.S256()
	e.PublicKey.X, e.PublicKey.Y = e.PublicKey.Curve.ScalarBaseMult(e.D.Bytes())
	return fmt.Sprintf("%x", elliptic.Marshal(secp256k1.S256(), e.X, e.Y))
}

func FromPublic(_publicKey string) (publicKey string) {
	var e ecdsa.PrivateKey
	e.D, _ = new(big.Int).SetString(_publicKey, 16)
	e.PublicKey.Curve = secp256k1.S256()
	e.PublicKey.X, e.PublicKey.Y = e.PublicKey.Curve.ScalarBaseMult(e.D.Bytes())
	return fmt.Sprintf("%x", elliptic.Marshal(secp256k1.S256(), e.X, e.Y))
}

func KeyFromPublic() {

}
