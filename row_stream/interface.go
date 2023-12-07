package row_stream

import (
	"context"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

// interfaces to encapsulate the streaming of rows from the plugin to the FDW
// - for GRPC connections Sender will be implemented by proto.WrapperPlugin_ExecuteServer)
// and Receiver by proto.WrapperPlugin_ExecuteClient)
// - for local plugin connections, Sender and Receiver will be implemented by localPluginStream (defined in FDW)

type Sender interface {
	Send(r *proto.ExecuteResponse) error
	Context() context.Context
}

type Receiver interface {
	Recv() (*proto.ExecuteResponse, error)
}
