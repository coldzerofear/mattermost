package main

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha512"
	"crypto/x509"
	_ "embed"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"os"
)

//go:embed rsa_private_key.pem
var privateKey []byte

//go:embed license.json
var rawLicense []byte

func main() {
	h := sha512.New()
	h.Write(rawLicense)
	hashed := h.Sum(nil)

	block, _ := pem.Decode(privateKey)
	if block == nil {
		fmt.Println("Failed to decode PEM block")
		return
	}

	var k *rsa.PrivateKey
	var err error

	// 尝试解析 PKCS#1 或 PKCS#8 格式 (根据你的密钥文件格式)
	if key, e := x509.ParsePKCS1PrivateKey(block.Bytes); e == nil {
		k = key
	} else if key, e := x509.ParsePKCS8PrivateKey(block.Bytes); e == nil {
		if rsaKey, ok := key.(*rsa.PrivateKey); ok {
			k = rsaKey
		} else {
			fmt.Println("Not an RSA private key in PKCS#8 format")
			return
		}
	} else {
		fmt.Println("Failed to parse private key")
		return
	}
	if err != nil {
		panic(err)
	}

	signature, err := rsa.SignPKCS1v15(nil, k, crypto.SHA512, hashed)
	result := append(rawLicense, signature...)
	// write base64 to file
	b64Result := base64.StdEncoding.EncodeToString(result)
	if err != nil {
		panic(err)
	}
	err = os.WriteFile("license-gen.txt", []byte(b64Result), 0644)
	if err != nil {
		panic(err)
	}
}
