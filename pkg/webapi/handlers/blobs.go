package handlers

import (
	"io"
	"net/http"
	"net/url"

	"github.com/legionus/kavka/pkg/context"
	"github.com/legionus/kavka/pkg/digest"
	"github.com/legionus/kavka/pkg/storage"
	"github.com/legionus/kavka/pkg/webapi"
)

func blobGetHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	p, ok := ctx.Value(webapi.HTTPRequestQueryParamsContextVar).(*url.Values)
	if !ok {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to obtain params from context")
		return
	}

	st, ok := ctx.Value(storage.AppStorageDriverContextVar).(storage.StorageDriver)
	if !ok {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to obtain params from context")
		return
	}

	dgst, err := digest.ParseDigest(p.Get("digest"))
	if err != nil {
		webapi.HTTPResponse(w, http.StatusBadRequest, "Bad digest: %s", err)
		return
	}

	blobReader, err := st.Reader(dgst)
	if err != nil {
		if err == storage.ErrBlobUnknown {
			webapi.HTTPResponse(w, http.StatusNotFound, "Not found: %s", dgst.String())
		} else {
			webapi.HTTPResponse(w, http.StatusInternalServerError, "%s", err)
		}
		return
	}

	if _, err := io.Copy(w, blobReader); err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "%s", err)
		return
	}

	blobReader.Close()
}
