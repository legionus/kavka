package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/Sirupsen/logrus"

	"github.com/legionus/kavka/pkg/context"
	"github.com/legionus/kavka/pkg/webapi"
	"github.com/legionus/kavka/pkg/webapi/middleware/jsonresponse"
)

type apiJSONError struct {
	Status int    `json:"status"`
	Title  string `json:"title"`
	Detail string `json:"detail"`
}

type apiJSONErrorOutOfRange struct {
	// HTTP status code.
	Code int `json:"code"`

	// Human readable error message.
	Message string `json:"message"`

	Topic        string `json:"topic"`
	Partition    int64  `json:"partition"`
	OffsetOldest int64  `json:"offsetfrom"`
	OffsetNewest int64  `json:"offsetto"`
}

func apiJSONHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	resp, ok := w.(*webapi.ResponseWriter)
	if !ok {
		panic("ResponseWriter is not webapi.ResponseWriter")
	}

	jsonresponse.Handler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) {
		data := &apiJSONError{
			Status: resp.HTTPStatus,
			Title:  http.StatusText(resp.HTTPStatus),
			Detail: resp.HTTPError,
		}
		b, err := json.Marshal(data)
		if err != nil {
			logrus.Errorln("Unable to marshal result:", err)
			return
		}
		w.Write(b)
	})(ctx, w, r)
}

func errorOutOfRange(ctx context.Context, w http.ResponseWriter, r *http.Request, topic string, partition int64, offsetFrom int64, offsetTo int64) {
	status := http.StatusRequestedRangeNotSatisfiable

	webapi.HTTPResponse(w, status, "Offset out of range (%v, %v)", offsetFrom, offsetTo)

	jsonresponse.Handler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) {
		data := &apiJSONErrorOutOfRange{
			Code:         status,
			Message:      fmt.Sprintf("Offset out of range (%v, %v)", offsetFrom, offsetTo),
			Topic:        topic,
			Partition:    partition,
			OffsetOldest: offsetFrom,
			OffsetNewest: offsetTo,
		}
		b, err := json.Marshal(data)
		if err != nil {
			logrus.Errorln("Unable to marshal result:", err)
			return
		}
		w.Write(b)
	})(ctx, w, r)
}

func internalServerErrorHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	_, ok := w.(*webapi.ResponseWriter)
	if !ok {
		w.Write([]byte(`Internal server error`))
		return
	}
	apiJSONHandler(ctx, w, r)
}

func notFoundHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	webapi.HTTPResponse(w, http.StatusNotFound, "Page not found")
	apiJSONHandler(ctx, w, r)
}

func notAllowedHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	webapi.HTTPResponse(w, http.StatusMethodNotAllowed, "Method Not Allowed")
	apiJSONHandler(ctx, w, r)
}

func pingHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	webapi.HTTPResponse(w, http.StatusOK, "OK")
	w.Write([]byte(""))
}
