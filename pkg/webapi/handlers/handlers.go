package handlers

import (
	"net/http"
	"regexp"

	"github.com/legionus/kavka/pkg/api"
	"github.com/legionus/kavka/pkg/context"
	"github.com/legionus/kavka/pkg/webapi"
	"github.com/legionus/kavka/pkg/webapi/middleware/jsonresponse"
)

type MethodHandlers map[string]webapi.Handler

type HandlerInfo struct {
	Regexp   *regexp.Regexp
	Handlers MethodHandlers
}

type EndpointsInfo struct {
	Endpoints []HandlerInfo
}

var Endpoints *EndpointsInfo = &EndpointsInfo{
	Endpoints: []HandlerInfo{
		{
			Regexp: regexp.MustCompile("^" + api.TopicsPath + "/(?P<topic>[A-Za-z0-9_-]+)/(?P<partition>[0-9]+)/?$"),
			Handlers: MethodHandlers{
				"GET":  topicGetHandler,
				"POST": jsonresponse.Handler(topicPostHandler),
			},
		},
		{
			Regexp: regexp.MustCompile("^" + api.InfoTopicsPath + "/(?P<topic>[A-Za-z0-9_-]+)/(?P<partition>[0-9]+)/?$"),
			Handlers: MethodHandlers{
				"GET": jsonresponse.Handler(infoSinglePartitionHandler),
			},
		},
		{
			Regexp: regexp.MustCompile("^" + api.InfoTopicsPath + "/(?P<topic>[A-Za-z0-9_-]+)/?$"),
			Handlers: MethodHandlers{
				"GET": jsonresponse.Handler(infoAllPartitionHandler),
			},
		},
		{
			Regexp: regexp.MustCompile("^" + api.InfoTopicsPath + "/?$"),
			Handlers: MethodHandlers{
				"GET": jsonresponse.Handler(infoTopicsHandler),
			},
		},
		{
			Regexp: regexp.MustCompile("^" + api.BlobsPath + "/(?P<digest>[a-zA-Z0-9]+:[a-zA-Z0-9]+)/?$"),
			Handlers: MethodHandlers{
				"GET": blobGetHandler,
			},
		},
		{
			Regexp: regexp.MustCompile("^" + api.JSONTopicsPath + "/(?P<topic>[A-Za-z0-9_-]+)/(?P<partition>[0-9]+)/?$"),
			Handlers: MethodHandlers{
				"GET":  jsonresponse.Handler(jsonGetHandler),
				"POST": jsonresponse.Handler(jsonPostHandler),
			},
		},
		{
			Regexp: regexp.MustCompile("^" + api.PingPath + "$"),
			Handlers: MethodHandlers{
				"GET": pingHandler,
			},
		},
		{
			Regexp: regexp.MustCompile("^" + api.EtcdMembersPath + "/(?P<memberid>[0-9]+)/?$"),
			Handlers: MethodHandlers{
				"POST":   jsonresponse.Handler(etcdUpdateHandler),
				"DELETE": jsonresponse.Handler(etcdDeleteHandler),
			},
		},
		{
			Regexp: regexp.MustCompile("^" + api.EtcdMembersPath + "$"),
			Handlers: MethodHandlers{
				"GET":  jsonresponse.Handler(etcdListHandler),
				"POST": jsonresponse.Handler(etcdAddHandler),
			},
		},
		{
			Regexp: regexp.MustCompile("^/"),
			Handlers: MethodHandlers{
				"GET": defultHandler,
			},
		},
	},
}

func Handler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	info, ok := ctx.Value(webapi.HTTPEndpointsContextVar).(*EndpointsInfo)
	if !ok {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to obtain API information from context")
		internalServerErrorHandler(ctx, w, r)
		return
	}

	p := r.URL.Query()

	for _, a := range info.Endpoints {
		match := a.Regexp.FindStringSubmatch(r.URL.Path)
		if match == nil {
			continue
		}

		for i, name := range a.Regexp.SubexpNames() {
			if i == 0 {
				continue
			}
			p.Set(name, match[i])
		}

		ctx = context.WithValue(ctx, webapi.HTTPRequestQueryParamsContextVar, &p)

		var reqHandler webapi.Handler

		if reqHandler, ok = a.Handlers[r.Method]; !ok {
			reqHandler = notAllowedHandler
		}

		reqHandler(ctx, w, r)
		return
	}

	// Never should be here
	notFoundHandler(ctx, w, r)
}
