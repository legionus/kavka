package jsonresponse

import (
	"fmt"
	"net/http"

	"github.com/legionus/kavka/pkg/context"
	"github.com/legionus/kavka/pkg/webapi"
)

func Handler(fn webapi.Handler) webapi.Handler {
	return func(ctx context.Context, resp http.ResponseWriter, req *http.Request) {
		resp.Header().Set("Content-Type", "application/json")
		resp.Write([]byte(`{"data":`))

		resplen := int64(0)
		if w, ok := resp.(*webapi.ResponseWriter); ok {
			resplen = w.ResponseLength
		}

		fn(ctx, resp, req)

		if w, ok := resp.(*webapi.ResponseWriter); ok {
			if w.ResponseLength == resplen {
				switch {
				case w.HTTPStatus == 416:
					w.Write([]byte(`{`))
					w.Write([]byte(w.HTTPError))
					w.Write([]byte(`}`))
				case w.HTTPStatus >= 400:
					w.Write([]byte(`{`))
					w.Write([]byte(fmt.Sprintf(`"status":%d,"title":"%s","detail":"%s"`,
						w.HTTPStatus,
						http.StatusText(w.HTTPStatus),
						w.HTTPError)))
					w.Write([]byte(`}`))
				default:
					w.Write([]byte(`{}`))
				}
			}
			if w.HTTPStatus < 400 {
				w.Write([]byte(`,"status":"success"`))
			} else {
				w.Write([]byte(`,"status":"error"`))
			}
		}

		resp.Write([]byte(`}`))
	}
}
