// Code generated by protoc-gen-go. DO NOT EDIT.
// source: synchronizer.proto

package gravity

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	grpc "google.golang.org/grpc"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

func init() { proto.RegisterFile("synchronizer.proto", fileDescriptor_f98557a2a36b798c) }

var fileDescriptor_f98557a2a36b798c = []byte{
	// 65 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x12, 0x2a, 0xae, 0xcc, 0x4b,
	0xce, 0x28, 0xca, 0xcf, 0xcb, 0xac, 0x4a, 0x2d, 0xd2, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62,
	0x4f, 0x2f, 0x4a, 0x2c, 0xcb, 0x2c, 0xa9, 0x34, 0xe2, 0xe3, 0xe2, 0x09, 0x46, 0x92, 0x4e, 0x62,
	0x03, 0xcb, 0x1b, 0x03, 0x02, 0x00, 0x00, 0xff, 0xff, 0xfd, 0x1a, 0x20, 0xa5, 0x35, 0x00, 0x00,
	0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// SynchronizerClient is the client API for Synchronizer service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type SynchronizerClient interface {
}

type synchronizerClient struct {
	cc *grpc.ClientConn
}

func NewSynchronizerClient(cc *grpc.ClientConn) SynchronizerClient {
	return &synchronizerClient{cc}
}

// SynchronizerServer is the server API for Synchronizer service.
type SynchronizerServer interface {
}

// UnimplementedSynchronizerServer can be embedded to have forward compatible implementations.
type UnimplementedSynchronizerServer struct {
}

func RegisterSynchronizerServer(s *grpc.Server, srv SynchronizerServer) {
	s.RegisterService(&_Synchronizer_serviceDesc, srv)
}

var _Synchronizer_serviceDesc = grpc.ServiceDesc{
	ServiceName: "gravity.Synchronizer",
	HandlerType: (*SynchronizerServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams:     []grpc.StreamDesc{},
	Metadata:    "synchronizer.proto",
}
