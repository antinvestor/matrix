package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"net/url"
	"os"

	"github.com/antinvestor/gomatrixserverlib"
	"github.com/antinvestor/gomatrixserverlib/fclient"
	"github.com/antinvestor/gomatrixserverlib/spec"
)

var requestFrom = flag.String("from", "", "the server name that the request should originate from")
var requestKey = flag.String("key", "matrix_key.pem", "the private key to use when signing the request")
var requestPost = flag.Bool("post", false, "send a POST request instead of GET (pipe input into stdin or type followed by Ctrl-D)")

func main() {
	flag.Parse()

	if requestFrom == nil || *requestFrom == "" {
		fmt.Println("expecting: furl -from origin.com [-key matrix_key.pem] https://path/to/url")
		fmt.Println("supported flags:")
		flag.PrintDefaults()
		os.Exit(1)
	}

	data, err := os.ReadFile(*requestKey)
	if err != nil {
		panic(err)
	}

	var privateKey ed25519.PrivateKey
	keyBlock, _ := pem.Decode(data)
	if keyBlock == nil {
		panic("keyBlock is nil")
	}
	if keyBlock.Type == "MATRIX PRIVATE KEY" {
		_, privateKey, err = ed25519.GenerateKey(bytes.NewReader(keyBlock.Bytes))
		if err != nil {
			panic(err)
		}
	} else {
		panic("unexpected key block")
	}

	serverName := spec.ServerName(*requestFrom)
	client := fclient.NewFederationClient(
		[]*fclient.SigningIdentity{
			{
				ServerName: serverName,
				KeyID:      gomatrixserverlib.KeyID(keyBlock.Headers["K-ID"]),
				PrivateKey: privateKey,
			},
		},
	)

	u, err := url.Parse(flag.Arg(0))
	if err != nil {
		panic(err)
	}

	var bodyObj interface{}
	var bodyBytes []byte
	method := "GET"
	if *requestPost {
		method = "POST"
		fmt.Println("Waiting for JSON input. Press Enter followed by Ctrl-D when done...")

		scan := bufio.NewScanner(os.Stdin)
		for scan.Scan() {
			scannedBytes := scan.Bytes()
			bodyBytes = append(bodyBytes, scannedBytes...)
		}
		fmt.Println("Done!")
		if err = json.Unmarshal(bodyBytes, &bodyObj); err != nil {
			panic(err)
		}
	}

	req := fclient.NewFederationRequest(
		method,
		serverName,
		spec.ServerName(u.Host),
		u.RequestURI(),
	)

	if *requestPost {
		if err = req.SetContent(bodyObj); err != nil {
			panic(err)
		}
	}

	if err = req.Sign(
		spec.ServerName(*requestFrom),
		gomatrixserverlib.KeyID(keyBlock.Headers["K-ID"]),
		privateKey,
	); err != nil {
		panic(err)
	}

	httpReq, err := req.HTTPRequest()
	if err != nil {
		panic(err)
	}

	var res interface{}
	err = client.DoRequestAndParseResponse(
		context.TODO(),
		httpReq,
		&res,
	)
	if err != nil {
		panic(err)
	}

	j, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		panic(err)
	}

	fmt.Println(string(j))
}
