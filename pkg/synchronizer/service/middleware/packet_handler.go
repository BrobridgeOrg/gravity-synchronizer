package middleware

import (
	"github.com/BrobridgeOrg/broc"
	packet_pb "github.com/BrobridgeOrg/gravity-api/packet"
	"github.com/golang/protobuf/proto"
)

func (m *Middleware) PacketHandler(ctx *broc.Context) (interface{}, error) {

	var packet packet_pb.Packet
	err := proto.Unmarshal(ctx.Get("request").([]byte), &packet)
	if err != nil {
		// invalid request
		return nil, nil
	}

	ctx.Set("request", &packet)

	//	return ctx.Next()

	// Preparing packet
	p := &packet_pb.Packet{}
	data, err := ctx.Next()
	if err != nil {
		p.Error = true
		p.Reason = err.Error()
		return proto.Marshal(p)
	}

	p.Payload = data.([]byte)

	return proto.Marshal(p)
}
