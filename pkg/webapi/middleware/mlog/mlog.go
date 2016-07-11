package mlog

import (
	"net/http"
	"time"

	"github.com/legionus/kavka/pkg/context"
	"github.com/legionus/kavka/pkg/logger"
	"github.com/legionus/kavka/pkg/webapi"
)

func Handler(fn webapi.Handler) webapi.Handler {
	return func(ctx context.Context, resp http.ResponseWriter, req *http.Request) {
		ctx = context.WithValue(ctx, webapi.HTTPRequestMethodContextVar, req.Method)
		ctx = context.WithValue(ctx, webapi.HTTPRequestRemoteAddrContextVar, req.RemoteAddr)
		ctx = context.WithValue(ctx, webapi.HTTPRequestLengthContextVar, req.ContentLength)
		ctx = context.WithValue(ctx, webapi.HTTPRequestTimeContextVar, time.Now().String())

		defer func() {
			e := logger.GetHTTPEntry(ctx)
			e = e.WithField(webapi.HTTPResponseTimeContextVar, time.Now().String())

			if w, ok := resp.(*webapi.ResponseWriter); ok {
				e = e.WithField(webapi.HTTPResponseLengthContextVar, w.ResponseLength)
				e = e.WithField(webapi.HTTPResponseStatusContextVar, w.HTTPStatus)
				e = e.WithField(webapi.HTTPResponseErrorContextVar, w.HTTPError)
			}
			e.Info(req.URL)
		}()

		fn(ctx, resp, req)
	}
}
