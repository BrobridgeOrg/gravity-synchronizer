package middleware

import (
	"errors"

	"github.com/BrobridgeOrg/broc"
	packet_pb "github.com/BrobridgeOrg/gravity-api/packet"
	"github.com/BrobridgeOrg/gravity-sdk/core/keyring"
	"github.com/golang/protobuf/proto"
)

func (m *Middleware) RequiredAuth(rules ...string) broc.Handler {

	auth, ok := m.middlewares["Authentication"].(*Authentication)
	if !ok {
		return nil
	}

	return auth.RequiredAuth(rules...)
}

type Authentication struct {
	Enabled bool
	Keyring *keyring.Keyring
}

func (auth *Authentication) RequiredAuth(rules ...string) broc.Handler {

	return func(ctx *broc.Context) (interface{}, error) {

		if !auth.Enabled {
			return ctx.Next()
		}

		packet := ctx.Get("request").(*packet_pb.Packet)

		// Using appID to find key info
		keyInfo := auth.Keyring.Get(packet.AppID)
		if keyInfo == nil {
			// No such app ID
			return nil, errors.New("InvalidKey")
		}

		// check permissions
		if len(rules) > 0 {
			hasPerm := false
			for _, rule := range rules {
				if keyInfo.Permission().Check(rule) {
					hasPerm = true
				}
			}

			// No permission
			if !hasPerm {
				return nil, errors.New("Forbidden")
			}
		}

		// Decrypt
		data, err := keyInfo.Encryption().Decrypt(packet.Payload)
		if err != nil {
			return nil, errors.New("InvalidKey")
		}

		// pass decrypted payload to next handler
		var payload packet_pb.Payload
		err = proto.Unmarshal(data, &payload)
		if err != nil {
			return nil, errors.New("InvalidPayload")
		}

		ctx.Set("payload", &payload)
		returnedData, err := ctx.Next()
		if err != nil {
			return nil, err
		}

		if returnedData == nil {
			return nil, errors.New("InternalError")
		}

		// Encrypt
		encrypted, err := keyInfo.Encryption().Encrypt(returnedData.([]byte))
		if err != nil {
			return nil, errors.New("InternalError")
		}

		return encrypted, nil
	}
}
