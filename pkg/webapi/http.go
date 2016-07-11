package webapi

import (
	"fmt"
	"net/http"

	"github.com/legionus/kavka/pkg/context"
)

const (
	HTTPEndpointsContextVar          = "http.endpoints"
	HTTPRequestQueryParamsContextVar = "http.request.query.params"

	HTTPRequestIDContextVar         = "http.request.id"
	HTTPRequestMethodContextVar     = "http.request.method"
	HTTPRequestRemoteAddrContextVar = "http.request.remoteaddr"
	HTTPRequestLengthContextVar     = "http.request.length"
	HTTPRequestTimeContextVar       = "http.request.time"
	HTTPResponseErrorContextVar     = "http.response.error"
	HTTPResponseStatusContextVar    = "http.response.status"
	HTTPResponseTimeContextVar      = "http.response.time"
	HTTPResponseLengthContextVar    = "http.response.length"
)

type Handler func(context.Context, http.ResponseWriter, *http.Request)

func IsAlive(w http.ResponseWriter) bool {
	closeNotify := w.(http.CloseNotifier).CloseNotify()
	select {
	case closed := <-closeNotify:
		if closed {
			return false
		}
	default:
	}
	return true
}

func HTTPResponse(w http.ResponseWriter, status int, format string, args ...interface{}) {
	err := ""
	if format != "" {
		err = fmt.Sprintf(format, args...)
	}

	if resp, ok := w.(*ResponseWriter); ok {
		resp.HTTPStatus = status
		resp.HTTPError = err
	}

	w.WriteHeader(status)
}
