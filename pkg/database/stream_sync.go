// Copyright Contributors to the Open Cluster Management project

package database

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/stolostron/search-indexer/pkg/model"
	"k8s.io/klog/v2"
)

// StreamSession manages state for a streaming sync operation
type StreamSession struct {
	SessionID      string
	ClusterName    string
	OverwriteState bool
	IsResync       bool
	UIDs           []interface{} // For tracking UIDs during resync
	batch          *batchWithRetry
	syncResponse   *model.SyncResponse
}

// StreamSyncChunk processes a single chunk of data for streaming sync
func (dao *DAO) StreamSyncChunk(ctx context.Context, session *StreamSession, chunk model.SyncEvent) error {
	// Process add resources
	for _, resource := range chunk.AddResources {
		if err := dao.processAddResource(ctx, session, resource); err != nil {
			return err
		}
	}

	// Process update resources  
	for _, resource := range chunk.UpdateResources {
		if err := dao.processUpdateResource(ctx, session, resource); err != nil {
			return err
		}
	}

	// Process delete resources
	for _, deleteEvent := range chunk.DeleteResources {
		if err := dao.processDeleteResource(ctx, session, deleteEvent); err != nil {
			return err
		}
	}

	// Process add edges
	for _, edge := range chunk.AddEdges {
		if err := dao.processAddEdge(ctx, session, edge); err != nil {
			return err
		}
	}

	// Process delete edges
	for _, edge := range chunk.DeleteEdges {
		if err := dao.processDeleteEdge(ctx, session, edge); err != nil {
			return err
		}
	}

	return nil
}

func (dao *DAO) processAddResource(ctx context.Context, session *StreamSession, resource model.Resource) error {
	data, _ := json.Marshal(resource.Properties)
	uid := resource.UID

	// Track UID for resync cleanup
	if session.IsResync {
		session.UIDs = append(session.UIDs, uid)
	}

	var query string
	var args []interface{}

	if session.IsResync {
		query = "INSERT into search.resources values($1,$2,$3) ON CONFLICT (uid) DO UPDATE SET data=$3 WHERE search.resources.data!=$3"
		args = []interface{}{uid, session.ClusterName, string(data)}
	} else {
		query = `INSERT into search.resources as r values($1,$2,$3) ON CONFLICT (uid) 
		DO UPDATE SET data=$3 WHERE r.uid=$1 and r.data IS DISTINCT FROM $3`
		args = []interface{}{uid, session.ClusterName, string(data)}
	}

	err := session.batch.Queue(batchItem{
		action: "addResource",
		query:  query,
		uid:    uid,
		args:   args,
	})

	if err == nil {
		session.syncResponse.TotalAdded++
	}

	return err
}

func (dao *DAO) processUpdateResource(ctx context.Context, session *StreamSession, resource model.Resource) error {
	data, _ := json.Marshal(resource.Properties)
	uid := resource.UID

	err := session.batch.Queue(batchItem{
		action: "updateResource",
		query:  "UPDATE search.resources SET data=$2 WHERE uid=$1",
		uid:    uid,
		args:   []interface{}{uid, string(data)},
	})

	if err == nil {
		session.syncResponse.TotalUpdated++
	}

	return err
}

func (dao *DAO) processDeleteResource(ctx context.Context, session *StreamSession, deleteEvent model.DeleteResourceEvent) error {
	uid := deleteEvent.UID

	// Delete resource
	err := session.batch.Queue(batchItem{
		action: "deleteResource",
		query:  "DELETE from search.resources WHERE uid=$1",
		uid:    uid,
		args:   []interface{}{uid},
	})

	if err != nil {
		return err
	}

	// Delete edges pointing to this resource
	err = session.batch.Queue(batchItem{
		action: "deleteResource",
		query:  "DELETE from search.edges WHERE sourceId=$1 OR destId=$1",
		uid:    uid,
		args:   []interface{}{uid},
	})

	if err == nil {
		session.syncResponse.TotalDeleted++
	}

	return err
}

func (dao *DAO) processAddEdge(ctx context.Context, session *StreamSession, edge model.Edge) error {
	err := session.batch.Queue(batchItem{
		action: "addEdge",
		query:  "INSERT into search.edges values($1,$2,$3,$4,$5,$6) ON CONFLICT (sourceid, destid, edgetype) DO NOTHING",
		uid:    edge.SourceUID,
		args:   []interface{}{edge.SourceUID, edge.SourceKind, edge.DestUID, edge.DestKind, edge.EdgeType, session.ClusterName},
	})

	if err == nil {
		session.syncResponse.TotalEdgesAdded++
	}

	return err
}

func (dao *DAO) processDeleteEdge(ctx context.Context, session *StreamSession, edge model.Edge) error {
	err := session.batch.Queue(batchItem{
		action: "deleteEdge",
		query:  "DELETE from search.edges WHERE sourceId=$1 AND destId=$2 AND edgeType=$3",
		uid:    edge.SourceUID,
		args:   []interface{}{edge.SourceUID, edge.DestUID, edge.EdgeType},
	})

	if err == nil {
		session.syncResponse.TotalEdgesDeleted++
	}

	return err
}

// StartStreamSession initializes a new streaming session
func (dao *DAO) StartStreamSession(ctx context.Context, sessionID, clusterName string, overwriteState bool, syncResponse *model.SyncResponse) *StreamSession {
	batch := NewBatchWithRetry(ctx, dao, syncResponse)
	session := &StreamSession{
		SessionID:      sessionID,
		ClusterName:    clusterName,
		OverwriteState: overwriteState,
		IsResync:       overwriteState,
		UIDs:           make([]interface{}, 0),
		batch:          &batch,
		syncResponse:   syncResponse,
	}

	return session
}

// FinalizeStreamSession completes the streaming session and handles resync cleanup
func (dao *DAO) FinalizeStreamSession(ctx context.Context, session *StreamSession) error {
	// Flush any remaining operations
	session.batch.flush()
	session.batch.wg.Wait()

	if session.batch.connError != nil {
		return session.batch.connError
	}

	// For resync operations, clean up resources that weren't included in the stream
	if session.IsResync && len(session.UIDs) > 0 {
		// Add cluster pseudo node to exclude from deletion
		session.UIDs = append(session.UIDs, fmt.Sprintf("cluster__%s", session.ClusterName))

		// Delete resources not in the incoming stream
		query, params, err := useGoqu(
			"DELETE from search.resources WHERE cluster=$1 AND uid NOT IN ($2)",
			[]interface{}{session.ClusterName, session.UIDs})
		if err == nil {
			cleanupBatch := NewBatchWithRetry(ctx, dao, session.syncResponse)
			queueErr := cleanupBatch.Queue(batchItem{
				action: "deleteResource",
				query:  query,
				uid:    fmt.Sprintf("%s", session.UIDs),
				args:   params,
			})
			if queueErr != nil {
				klog.Warningf("Error queuing resources for deletion during cleanup. Error: %+v", queueErr)
			}

			// Delete orphaned edges
			query, params, err = useGoqu(
				"DELETE from search.edges WHERE cluster=$1 AND sourceid NOT IN ($2) OR destid NOT IN ($2)",
				[]interface{}{session.ClusterName, session.UIDs})
			if err == nil {
				queueErr = cleanupBatch.Queue(batchItem{
					action: "deleteEdge",
					query:  query,
					uid:    fmt.Sprintf("%s", session.UIDs),
					args:   params,
				})
				if queueErr != nil {
					klog.Warningf("Error queuing edges for deletion during cleanup. Error: %+v", queueErr)
				}
			}

			cleanupBatch.flush()
			cleanupBatch.wg.Wait()

			if cleanupBatch.connError != nil {
				return cleanupBatch.connError
			}
		}
	}

	return nil
}