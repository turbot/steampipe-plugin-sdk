
#rebuild the protobuf type definitions
protoc:
	protoc -I ./grpc/proto/ ./grpc/proto/plugin.proto --go_out=plugins=grpc:./grpc/proto/

