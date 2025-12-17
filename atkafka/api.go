package atkafka

import (
	"context"
	"fmt"
	"time"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/xrpc"
	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/time/rate"
)

type ApiClient struct {
	xrpcClient *xrpc.Client

	profileCache   *lru.LRU[string, *bsky.ActorDefs_ProfileViewDetailed]
	profileLimiter *rate.Limiter
}

type ApiClientArgs struct {
	ApiHost string
}

func NewApiClient(args *ApiClientArgs) (*ApiClient, error) {
	xrpcClient := xrpc.Client{
		Host: args.ApiHost,
	}

	pc := ApiClient{
		xrpcClient: &xrpcClient,
		profileCache: lru.NewLRU(100_000, func(key string, value *bsky.ActorDefs_ProfileViewDetailed) {
			cacheSize.WithLabelValues("profile").Dec()
		}, 10*time.Minute),
		profileLimiter: rate.NewLimiter(rate.Limit(200), 100),
	}

	return &pc, nil
}

func (pc *ApiClient) GetProfile(ctx context.Context, did string) (*bsky.ActorDefs_ProfileViewDetailed, error) {
	status := "error"
	cached, ok := pc.profileCache.Get(did)

	defer func() {
		apiRequests.WithLabelValues("profile", status, fmt.Sprintf("%t", ok)).Inc()
	}()

	if ok {
		status = "ok"
		return cached, nil
	}

	if err := pc.profileLimiter.Wait(ctx); err != nil {
		status = "limited"
		return nil, fmt.Errorf("failed to get limit: %w", err)
	}

	resp, err := bsky.ActorGetProfile(ctx, pc.xrpcClient, did)
	if err != nil {
		return nil, fmt.Errorf("error getting profile: %w", err)
	}

	// add a carveout for not caching profiles of those who have extremely new accounts (low followers count, low post count)
	// note that these will still get cached by the public api's CDN so lookups will be cheap and fast
	if (resp.FollowersCount != nil && *resp.FollowersCount > 10) && (resp.PostsCount != nil && *resp.PostsCount > 10) {
		pc.profileCache.Add(did, resp)
		cacheSize.WithLabelValues("profile").Inc()
	}

	status = "ok"

	return resp, nil
}

func (pc *ApiClient) BustProfileCache(did string) bool {
	return pc.profileCache.Remove(did)
}
