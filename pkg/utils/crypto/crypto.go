// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package crypto

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"math/big"
	"open_im_sdk/pkg/utils/crypto/ecies"
	"os"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/crypto/secp256k1"
	"github.com/ethereum/go-ethereum/rlp"
	"golang.org/x/crypto/sha3"
)

//SignatureLength indicates the byte length required to carry a signature with recovery id.
const SignatureLength = 64 + 1 // 64 bytes ECDSA signature + 1 byte recovery id

// RecoveryIDOffset points to the byte offset within the signature that contains the recovery id.
const RecoveryIDOffset = 64

// DigestLength sets the signature digest exact length
const DigestLength = 32

var (
	secp256k1N, _  = new(big.Int).SetString("fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141", 16)
	secp256k1halfN = new(big.Int).Div(secp256k1N, big.NewInt(2))
)

var errInvalidPubkey = errors.New("invalid secp256k1 public key")

// KeccakState wraps sha3.state. In addition to the usual hash methods, it also supports
// Read to get a variable amount of data from the hash state. Read is faster than Sum
// because it doesn't copy the internal state, but also modifies the internal state.
type KeccakState interface {
	hash.Hash
	Read([]byte) (int, error)
}

// NewKeccakState creates a new KeccakState
func NewKeccakState() KeccakState {
	return sha3.NewLegacyKeccak256().(KeccakState)
}

// HashData hashes the provided data using the KeccakState and returns a 32 byte hash
func HashData(kh KeccakState, data []byte) (h common.Hash) {
	kh.Reset()
	kh.Write(data)
	kh.Read(h[:])
	return h
}

// Keccak256 calculates and returns the Keccak256 hash of the input data.
func Keccak256(data ...[]byte) []byte {
	b := make([]byte, 32)
	d := NewKeccakState()
	for _, b := range data {
		d.Write(b)
	}
	d.Read(b)
	return b
}

// Keccak256Hash calculates and returns the Keccak256 hash of the input data,
// converting it to an internal Hash data structure.
func Keccak256Hash(data ...[]byte) (h common.Hash) {
	d := NewKeccakState()
	for _, b := range data {
		d.Write(b)
	}
	d.Read(h[:])
	return h
}

// Keccak512 calculates and returns the Keccak512 hash of the input data.
func Keccak512(data ...[]byte) []byte {
	d := sha3.NewLegacyKeccak512()
	for _, b := range data {
		d.Write(b)
	}
	return d.Sum(nil)
}

// CreateAddress creates an ethereum address given the bytes and the nonce
func CreateAddress(b common.Address, nonce uint64) common.Address {
	data, _ := rlp.EncodeToBytes([]interface{}{b, nonce})
	return common.BytesToAddress(Keccak256(data)[12:])
}

// CreateAddress2 creates an ethereum address given the address bytes, initial
// contract code hash and a salt.
func CreateAddress2(b common.Address, salt [32]byte, inithash []byte) common.Address {
	return common.BytesToAddress(Keccak256([]byte{0xff}, b.Bytes(), salt[:], inithash)[12:])
}

// ToECDSA creates a private key with the given D value.
func ToECDSA(d []byte) (*ecdsa.PrivateKey, error) {
	return toECDSA(d, true)
}

// ToECDSAUnsafe blindly converts a binary blob to a private key. It should almost
// never be used unless you are sure the input is valid and want to avoid hitting
// errors due to bad origin encoding (0 prefixes cut off).
func ToECDSAUnsafe(d []byte) *ecdsa.PrivateKey {
	priv, _ := toECDSA(d, false)
	return priv
}

// toECDSA creates a private key with the given D value. The strict parameter
// controls whether the key's length should be enforced at the curve size or
// it can also accept legacy encodings (0 prefixes).
func toECDSA(d []byte, strict bool) (*ecdsa.PrivateKey, error) {
	priv := new(ecdsa.PrivateKey)
	priv.PublicKey.Curve = S256()
	if strict && 8*len(d) != priv.Params().BitSize {
		return nil, fmt.Errorf("invalid length, need %d bits", priv.Params().BitSize)
	}
	priv.D = new(big.Int).SetBytes(d)

	// The priv.D must < N
	if priv.D.Cmp(secp256k1N) >= 0 {
		return nil, fmt.Errorf("invalid private key, >=N")
	}
	// The priv.D must not be zero or negative.
	if priv.D.Sign() <= 0 {
		return nil, fmt.Errorf("invalid private key, zero or negative")
	}

	priv.PublicKey.X, priv.PublicKey.Y = priv.PublicKey.Curve.ScalarBaseMult(d)
	if priv.PublicKey.X == nil {
		return nil, errors.New("invalid private key")
	}
	return priv, nil
}

// FromECDSA exports a private key into a binary dump.
func FromECDSA(priv *ecdsa.PrivateKey) []byte {
	if priv == nil {
		return nil
	}
	return math.PaddedBigBytes(priv.D, priv.Params().BitSize/8)
}

// UnmarshalPubkey converts bytes to a secp256k1 public key.
func UnmarshalPubkey(pub []byte) (*ecdsa.PublicKey, error) {
	x, y := elliptic.Unmarshal(S256(), pub)
	if x == nil {
		return nil, errInvalidPubkey
	}
	return &ecdsa.PublicKey{Curve: S256(), X: x, Y: y}, nil
}

func FromECDSAPub(pub *ecdsa.PublicKey) []byte {
	if pub == nil || pub.X == nil || pub.Y == nil {
		return nil
	}
	return elliptic.Marshal(S256(), pub.X, pub.Y)
}

// HexToECDSA parses a secp256k1 private key.
func HexToECDSA(hexkey string) (*ecdsa.PrivateKey, error) {
	b, err := hex.DecodeString(hexkey)
	if byteErr, ok := err.(hex.InvalidByteError); ok {
		return nil, fmt.Errorf("invalid hex character %q in private key", byte(byteErr))
	} else if err != nil {
		return nil, errors.New("invalid hex data for private key")
	}
	return ToECDSA(b)
}

// LoadECDSA loads a secp256k1 private key from the given file.
func LoadECDSA(file string) (*ecdsa.PrivateKey, error) {
	fd, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	r := bufio.NewReader(fd)
	buf := make([]byte, 64)
	n, err := readASCII(buf, r)
	if err != nil {
		return nil, err
	} else if n != len(buf) {
		return nil, fmt.Errorf("key file too short, want 64 hex characters")
	}
	if err := checkKeyFileEnd(r); err != nil {
		return nil, err
	}

	return HexToECDSA(string(buf))
}

// readASCII reads into 'buf', stopping when the buffer is full or
// when a non-printable control character is encountered.
func readASCII(buf []byte, r *bufio.Reader) (n int, err error) {
	for ; n < len(buf); n++ {
		buf[n], err = r.ReadByte()
		switch {
		case err == io.EOF || buf[n] < '!':
			return n, nil
		case err != nil:
			return n, err
		}
	}
	return n, nil
}

// checkKeyFileEnd skips over additional newlines at the end of a key file.
func checkKeyFileEnd(r *bufio.Reader) error {
	for i := 0; ; i++ {
		b, err := r.ReadByte()
		switch {
		case err == io.EOF:
			return nil
		case err != nil:
			return err
		case b != '\n' && b != '\r':
			return fmt.Errorf("invalid character %q at end of key file", b)
		case i >= 2:
			return errors.New("key file too long, want 64 hex characters")
		}
	}
}

// SaveECDSA saves a secp256k1 private key to the given file with
// restrictive permissions. The key data is saved hex-encoded.
func SaveECDSA(file string, key *ecdsa.PrivateKey) error {
	k := hex.EncodeToString(FromECDSA(key))
	return os.WriteFile(file, []byte(k), 0600)
}

// GenerateKey generates a new private key.
func GenerateKey() (*ecdsa.PrivateKey, error) {
	return ecdsa.GenerateKey(S256(), rand.Reader)
}

// ValidateSignatureValues verifies whether the signature values are valid with
// the given chain rules. The v value is assumed to be either 0 or 1.
func ValidateSignatureValues(v byte, r, s *big.Int, homestead bool) bool {
	if r.Cmp(common.Big1) < 0 || s.Cmp(common.Big1) < 0 {
		return false
	}
	// reject upper range of s values (ECDSA malleability)
	// see discussion in secp256k1/libsecp256k1/include/secp256k1.h
	if homestead && s.Cmp(secp256k1halfN) > 0 {
		return false
	}
	// Frontier: allow s to be in full N range
	return r.Cmp(secp256k1N) < 0 && s.Cmp(secp256k1N) < 0 && (v == 0 || v == 1)
}

func PubkeyToAddress(p ecdsa.PublicKey) common.Address {
	pubBytes := FromECDSAPub(&p)
	return common.BytesToAddress(Keccak256(pubBytes[1:])[12:])
}

func zeroBytes(bytes []byte) {
	for i := range bytes {
		bytes[i] = 0
	}
}

// ===================================

/**
 * return whith 04
 */
func Public(privateKey string) (publicKey []byte) {
	var e ecdsa.PrivateKey
	e.D, _ = new(big.Int).SetString(privateKey, 16)
	e.PublicKey.Curve = secp256k1.S256()
	e.PublicKey.X, e.PublicKey.Y = e.PublicKey.Curve.ScalarBaseMult(e.D.Bytes())
	// fmt.Printf("X: %d, Y: %d", e.PublicKey.X, e.PublicKey.Y)
	return elliptic.Marshal(secp256k1.S256(), e.X, e.Y)
}

func hexToPrivateKey(hexStr string) *ecdsa.PrivateKey {
	// bytes, err := hex.DecodeString(hexStr)
	// print(err)

	k := new(big.Int)
	// k.SetBytes(bytes)
	k.SetString(hexStr, 16)

	priv := new(ecdsa.PrivateKey)
	// curve := elliptic.P256()
	curve := secp256k1.S256()
	priv.PublicKey.Curve = curve
	priv.D = k
	priv.PublicKey.X, priv.PublicKey.Y = curve.ScalarBaseMult(k.Bytes())
	//this print can be used to verify if we got the same parameters as in Java version
	// fmt.Printf("X: %d, Y: %d", priv.PublicKey.X, priv.PublicKey.Y)

	return priv
}

func hexToPublicKey(xHex string, yHex string) *ecdsa.PublicKey {
	xBytes, _ := hex.DecodeString(xHex)
	x := new(big.Int)
	x.SetBytes(xBytes)

	yBytes, _ := hex.DecodeString(yHex)
	y := new(big.Int)
	y.SetBytes(yBytes)

	pub := new(ecdsa.PublicKey)
	pub.X = x
	pub.Y = y

	pub.Curve = elliptic.P256()

	return pub
}

func fromPublicKey(pubKey string) *ecdsa.PublicKey {
	pubKeyBytes, _ := hex.DecodeString(pubKey)
	pub := new(ecdsa.PublicKey)

	curve := secp256k1.S256()

	var x, y *big.Int

	x, y = elliptic.Unmarshal(curve, pubKeyBytes)
	if x == nil || y == nil {
		return pub
	}
	pub.X = x
	pub.Y = y

	pub.Curve = curve

	return pub

}

func GenerateSharedSecret(privKey ecdsa.PrivateKey, pubKey ecdsa.PublicKey) ([]byte, error) {
	// priv := privKey.(*PrivateKey)
	// pub := pubKey.(*PublicKey)
	k := (privKey.D).Bytes()
	if pubKey.X == nil || pubKey.Y == nil {
		return nil, errors.New("pubKey invalid")
	}

	x, _ := privKey.ScalarMult(pubKey.X, pubKey.Y, k)
	return x.Bytes(), nil
}

func GenRandomBytes(size int) (blk []byte, err error) {
	blk = make([]byte, size)
	_, err = rand.Read(blk)
	return
}

func Encrypt(pubKeyTo string, plaintext string, privKey string) []byte {
	var prvKey *ecdsa.PrivateKey
	var pubKeyBuff []byte
	if privKey == "" {
		prvKey, _ = GenerateKey()

		pubKeyBuff = elliptic.Marshal(secp256k1.S256(), prvKey.PublicKey.X, prvKey.PublicKey.Y)
	} else {
		prvKey = hexToPrivateKey(privKey)
		pubKeyBuff = Public(privKey)
	}
	pubKey := fromPublicKey(pubKeyTo)
	// pubKeyBuff := elliptic.Marshal(secp256k1.S256(), pubKey.X, pubKey.Y)
	// pubKeyBuff, _ := hex.DecodeString(pubKeyTo)

	shareKey, _ := GenerateSharedSecret(*prvKey, *pubKey)

	hash := sha512.Sum512([]byte(shareKey))

	iv, _ := GenRandomBytes(16)

	// iv = []byte{186, 43, 16, 120, 107, 183, 38, 126, 245, 37, 21, 113, 16, 103, 185, 95}

	encryptionKey := hash[0:32]
	macKey := hash[32:]

	ciphertext, _ := ecies.Ase256(plaintext, string(encryptionKey), string(iv), aes.BlockSize)

	var buffer bytes.Buffer

	buffer.Write(iv)
	buffer.Write(pubKeyBuff)
	ct, _ := hex.DecodeString(ciphertext)

	buffer.Write(ct)

	dataToMac := buffer.Bytes() // get b1+b2 result

	// Create a new HMAC by defining the hash type and the key (as byte array)
	h := hmac.New(sha256.New, macKey)

	// Write Data to it
	h.Write(dataToMac)

	// Get result and encode as hexadecimal string
	mac := hex.EncodeToString(h.Sum(nil))

	var serializedCiphertextBuffer bytes.Buffer

	serializedCiphertextBuffer.Write(iv)
	serializedCiphertextBuffer.Write(pubKeyBuff)
	macBuff, _ := hex.DecodeString(mac)

	serializedCiphertextBuffer.Write(macBuff)
	serializedCiphertextBuffer.Write(ct)

	serializedCiphertext := serializedCiphertextBuffer.Bytes() // get b1+b2 result

	// return hex.EncodeToString(serializedCiphertext)
	return serializedCiphertext
}

// privKey string 2 byte
// encrypted byte
// publicKeyFrom with 04
func Decrypt(privKey, encrypted []byte) string {
	iv := encrypted[0:16]

	// var buffer bytes.Buffer
	// buffer.Write([]byte("04"))
	// buffer.Write(publicKeyFrom)
	// ephemPubKeyEncoded := buffer.Bytes()

	ephemPubKeyEncoded := encrypted[16:81]
	mac := encrypted[81:113]
	ciphertext := encrypted[113:]

	ephemPubKey := fromPublicKey(hex.EncodeToString(ephemPubKeyEncoded))

	prvKey := hexToPrivateKey(string(privKey))
	// px
	shareKey, _ := GenerateSharedSecret(*prvKey, *ephemPubKey)

	hash := sha512.Sum512([]byte(shareKey))

	encryptionKey := hash[0:32]
	macKey := hash[32:]

	var macBuffer bytes.Buffer //Buffer是一个实现了读写方法的可变大小的字节缓冲

	macBuffer.Write(iv)
	macBuffer.Write(ephemPubKeyEncoded)
	macBuffer.Write(ciphertext)
	dataToMac := macBuffer.Bytes()

	// Create a new HMAC by defining the hash type and the key (as byte array)
	h := hmac.New(sha256.New, macKey)

	// Write Data to it
	h.Write(dataToMac)

	// Get result and encode as hexadecimal string
	computedMacBuff := h.Sum(nil)

	if !bytes.Equal(computedMacBuff, mac) {
		fmt.Println("MAC mismatch")
		return ""
	}

	plaintext := ecies.Ase256Decode(hex.EncodeToString(ciphertext), string(encryptionKey), string(iv))

	return plaintext

}

func ephemPubKeyEncodedFromEncrypted(encrypted []byte) []byte {
	return encrypted[16:81]
}

func verifyPubKey(encrypted []byte, publicKeyFrom []byte) bool {
	return bytes.Equal(ephemPubKeyEncodedFromEncrypted(encrypted), publicKeyFrom)
}
