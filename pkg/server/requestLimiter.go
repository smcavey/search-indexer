// Copyright Contributors to the Open Cluster Management project

package server

import (
	"net/http"
	"sync"
	"time"

	klog "k8s.io/klog/v2"

	"github.com/gorilla/mux"
	"github.com/stolostron/search-indexer/pkg/config"
)

var requestTracker = map[string]time.Time{}
var requestTrackerLock = sync.RWMutex{}

// Checks if we are able to accept the incoming request.
func requestLimiterMiddleware(next http.Handler) http.Handler {

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)
		clusterName := params["id"]

		requestTrackerLock.RLock()
		requestCount := len(requestTracker)
		klog.V(6).Info("Checking if we can process incoming request. Current requests: ", requestCount)
		timeReqReceived, foundClusterProcessing := requestTracker[clusterName]
		requestTrackerLock.RUnlock()

		if foundClusterProcessing {
			klog.Warningf("Rejecting request from %s because there's a previous request processing. Duration: %s",
				clusterName, time.Since(timeReqReceived))
			http.Error(w, "A previous request from this cluster is processing, retry later.", http.StatusTooManyRequests)
			return
		}

		// Give higher priority to requests from the collector in the hub cluster.
		//   request host is stored as plain text service when originating from hub cluster
		//   request host uses internal IP when coming from proxy service, i.e. managed clusters
		hubClusterReq := r.Host == "search-indexer.open-cluster-management.svc:3010"
		if requestCount >= config.Cfg.RequestLimit && !hubClusterReq {
			klog.Warningf("Too many pending requests (%d). Rejecting sync from %s", requestCount, clusterName)
			http.Error(w, "Indexer has too many pending requests, retry later.", http.StatusTooManyRequests)
			return
		}

		requestTrackerLock.Lock()
		requestTracker[clusterName] = time.Now()
		requestTrackerLock.Unlock()

		defer func() { // Using defer to guarantee this gets executed if there's an error processing the request.
			requestTrackerLock.Lock()
			delete(requestTracker, clusterName)
			requestTrackerLock.Unlock()
		}()

		next.ServeHTTP(w, r)
	})
}
