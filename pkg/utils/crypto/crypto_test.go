package crypto

import (
	"bytes"
	"crypto/aes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"open_im_sdk/pkg/utils/crypto/ecies"
	"testing"
)

func TestEncrypt(t *testing.T) {
	const PRIVATE = "1000000000000000000000000000000000000000000000000000000000000000"
	const PRIVATE2 = "2000000000000000000000000000000000000000000000000000000000000000"
	var pubKeyBuff = Public(PRIVATE)
	fmt.Println("publicKey: \t", fmt.Sprintf("%x", pubKeyBuff))

	// https://github.com/wsddn/go-ecdh/blob/48726bab92085232373de4ec5c51ce7b441c63a0/elliptic.go#L10

	var prvKey = hexToPrivateKey(PRIVATE)

	// fmt.Println("prvKey: \t", prvKey)

	var prvKey2 = hexToPrivateKey(PRIVATE2)
	// fmt.Println("prvKey2: \t", prvKey2)

	// const PUBLIC = "04bc3c5053c9d3143861eb1e8322d268c1da5c3aa2b5a42343dfcdba9a0317a1ee866a0d2834c69325d2a72ff8aad310395c178903b3b4719cab86736851889dcf"
	// pubKey4 := fromPublicKey(PUBLIC)

	// fmt.Println("pubKey4: \t", pubKey4)

	// pubKey41, _ := UnmarshalPubkey([]byte(PUBLIC))

	// fmt.Println("pubKey41: \t", pubKey41)

	pubKey := prvKey.PublicKey
	pubKey2 := prvKey2.PublicKey

	shareKey, err3 := GenerateSharedSecret(*prvKey, pubKey2)
	if err3 != nil {
		fmt.Println(err3)
	}

	fmt.Println("shareKey: \t", shareKey)
	fmt.Println("shareKey hex: \t", fmt.Sprintf("%x", shareKey))

	shareKey2, err4 := GenerateSharedSecret(*prvKey2, pubKey)
	if err4 != nil {
		fmt.Println(err4)
	}
	fmt.Println("shareKey2: \t", shareKey2)
	fmt.Println("shareKey2 hex: \t", fmt.Sprintf("%x", shareKey2))

	hash := sha512.Sum512([]byte(shareKey))

	fmt.Println("hash: \t", fmt.Sprintf("%x", hash))

	iv, err5 := GenRandomBytes(16)
	if err5 != nil {
		fmt.Println(err5)
	}
	fmt.Println("iv: \t", fmt.Sprintf("%x", iv))
	// iv = []byte("97790e46cf346320ad5c6bf0234f3b65")
	iv = []byte{186, 43, 16, 120, 107, 183, 38, 126, 245, 37, 21, 113, 16, 103, 185, 95}

	const plaintext = "hello world"
	encryptionKey := hash[0:32]
	fmt.Println("encryptionKey: \t", fmt.Sprintf("%x", encryptionKey))
	macKey := hash[32:]
	fmt.Println("macKey: \t", fmt.Sprintf("%x", macKey))

	ciphertext, err6 := ecies.Ase256(plaintext, string(encryptionKey), string(iv), aes.BlockSize)
	if err6 != nil {
		fmt.Println(err5)
	}
	fmt.Println("ciphertext: \t", ciphertext)

	var buffer bytes.Buffer //Buffer是一个实现了读写方法的可变大小的字节缓冲

	buffer.Write(iv)
	buffer.Write(pubKeyBuff)
	ct, err := hex.DecodeString(ciphertext)
	if err != nil {
		fmt.Println(err)
		return
	}
	buffer.Write(ct)

	dataToMac := buffer.Bytes()

	fmt.Println("dataToMac: \t", fmt.Sprintf("%x", dataToMac))

	// Create a new HMAC by defining the hash type and the key (as byte array)
	h := hmac.New(sha256.New, macKey)

	// Write Data to it
	h.Write(dataToMac)

	// Get result and encode as hexadecimal string
	mac := hex.EncodeToString(h.Sum(nil))

	fmt.Println("mac: " + mac)

	var serializedCiphertextBuffer bytes.Buffer

	serializedCiphertextBuffer.Write(iv)
	serializedCiphertextBuffer.Write(pubKeyBuff)
	macBuff, err7 := hex.DecodeString(mac)
	if err7 != nil {
		fmt.Println(err7)
		return
	}
	serializedCiphertextBuffer.Write(macBuff)
	serializedCiphertextBuffer.Write(ct)

	serializedCiphertext := serializedCiphertextBuffer.Bytes()

	fmt.Println("serializedCiphertextBuffer： \t", hex.EncodeToString(serializedCiphertext))

	plainText := Decrypt([]byte(PRIVATE2), serializedCiphertext)

	fmt.Println(plainText)

}

// Refer: https://github.com/libertylocked/eth-ecies/blob/master/index.js
func TestDecrypt(t *testing.T) {
	const PRIVATE = "1000000000000000000000000000000000000000000000000000000000000000"
	const PRIVATE2 = "2000000000000000000000000000000000000000000000000000000000000000"

	// var pubKeyBuff = Public(PRIVATE)
	var pubKeyTo = Public(PRIVATE2)

	plaintext := "hello world"
	serializedCiphertext := Encrypt(hex.EncodeToString(pubKeyTo), plaintext, PRIVATE)

	// fmt.Println("serializedCiphertext: \t", hex.EncodeToString(serializedCiphertext))

	// textBuff, _ := hex.DecodeString(serializedCiphertext)

	decrypted := Decrypt([]byte(PRIVATE2), serializedCiphertext)

	fmt.Println(decrypted)

	if decrypted != plaintext {
		t.Fatalf("plaintext should : %s ", plaintext)
	}
}
