package query_cache

import (
	"fmt"
	"github.com/gertd/go-pluralize"
)

type SubscriberError struct {
	rowsStreamed    int
	callId          string
	publisherCallId string
	underlying      error
}

func newSubscriberError(err error, s *setRequestSubscriber) error {
	return SubscriberError{
		rowsStreamed:    s.rowsStreamed,
		callId:          s.callId,
		publisherCallId: s.publisher.CallId,
		underlying:      err,
	}
}

func (e SubscriberError) Error() string {
	streamedString := " (no rows streamed)"
	if e.rowsStreamed > 0 {
		streamedString = fmt.Sprintf(" (already streamed %d %s)",
			e.rowsStreamed,
			pluralize.NewClient().Pluralize("row", e.rowsStreamed, false))
	}

	return fmt.Sprintf("%s%s", e.Error(), streamedString)
}
