package handlers

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/legionus/kavka/pkg/config"
	"github.com/legionus/kavka/pkg/context"
	"github.com/legionus/kavka/pkg/etcd"
	"github.com/legionus/kavka/pkg/util"
	"github.com/legionus/kavka/pkg/webapi"
)

type etcdMembers struct {
	Peers []string `json:"peerURLs"`
}

func etcdListHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	cfg, ok := ctx.Value(config.AppConfigContextVar).(*config.Config)
	if !ok {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to obtain config from context")
		return
	}

	client, err := etcd.NewEtcdClient(cfg)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to create etcd client: %s", err)
		return
	}

	resp, err := client.MemberList(ctx)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to get member list: %s", err)
		return
	}

	res, err := json.Marshal(resp.Members)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to marshal json: %v", err)
		return
	}

	w.Write(res)
}

func etcdAddHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	cfg, ok := ctx.Value(config.AppConfigContextVar).(*config.Config)
	if !ok {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to obtain config from context")
		return
	}

	client, err := etcd.NewEtcdClient(cfg)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to create etcd client: %s", err)
		return
	}

	var stream io.Reader = r.Body

	if cfg.Topic.MaxMessageSize > 0 {
		stream = &io.LimitedReader{
			R: r.Body,
			N: cfg.Topic.MaxMessageSize,
		}
	}

	msg, err := ioutil.ReadAll(stream)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to read body: %s", err)
		return
	}

	var m etcdMembers

	if err = json.Unmarshal(msg, &m); err != nil {
		webapi.HTTPResponse(w, http.StatusBadRequest, "Message must be JSON")
		return
	}

	resp, err := client.MemberAdd(ctx, m.Peers)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to add member: %s", err)
		return
	}

	for _, clientURL := range resp.Member.ClientURLs {
		if util.InSliceString(clientURL, cfg.Etcd.Client.URLs) {
			continue
		}
		cfg.Etcd.Client.URLs = append(cfg.Etcd.Client.URLs, clientURL)
	}

	res, err := json.Marshal(resp.Member)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to marshal json: %v", err)
		return
	}

	w.Write(res)
}

func etcdUpdateHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	p, ok := ctx.Value(webapi.HTTPRequestQueryParamsContextVar).(*url.Values)
	if !ok {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to obtain params from context")
		return
	}

	cfg, ok := ctx.Value(config.AppConfigContextVar).(*config.Config)
	if !ok {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to obtain config from context")
		return
	}

	client, err := etcd.NewEtcdClient(cfg)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to create etcd client: %s", err)
		return
	}

	var stream io.Reader = r.Body

	if cfg.Topic.MaxMessageSize > 0 {
		stream = &io.LimitedReader{
			R: r.Body,
			N: cfg.Topic.MaxMessageSize,
		}
	}

	msg, err := ioutil.ReadAll(stream)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to read body: %s", err)
		return
	}

	var m etcdMembers

	if err = json.Unmarshal(msg, &m); err != nil {
		webapi.HTTPResponse(w, http.StatusBadRequest, "Message must be JSON")
		return
	}

	id := util.ToUint64(p.Get("memberid"))

	_, err = client.MemberUpdate(ctx, id, m.Peers)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to update member: %s", err)
		return
	}

	w.Write([]byte("OK"))
}

func etcdDeleteHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	p, ok := ctx.Value(webapi.HTTPRequestQueryParamsContextVar).(*url.Values)
	if !ok {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to obtain params from context")
		return
	}

	cfg, ok := ctx.Value(config.AppConfigContextVar).(*config.Config)
	if !ok {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to obtain config from context")
		return
	}

	client, err := etcd.NewEtcdClient(cfg)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to create etcd client: %s", err)
		return
	}

	id := util.ToUint64(p.Get("memberid"))

	resp, err := client.MemberList(ctx)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to get member list: %s", err)
		return
	}

	_, err = client.MemberRemove(ctx, id)
	if err != nil {
		webapi.HTTPResponse(w, http.StatusInternalServerError, "Unable to remove member: %s", err)
		return
	}

	for _, member := range resp.Members {
		if id != member.ID {
			continue
		}
		var endpoints []string
		for _, clientURL := range cfg.Etcd.Client.URLs {
			if util.InSliceString(clientURL, member.ClientURLs) {
				continue
			}
			endpoints = append(endpoints, clientURL)
		}
		cfg.Etcd.Client.URLs = endpoints
	}

	w.Write([]byte("OK"))
}
