// Package query implements application-layer queries for rag-notification-dispatch-daemon.
package query

import (
	"context"
	"errors"
	"time"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/transport/hub"
)

// ActiveSessionDTO represents the API response schema for a single SSE connection.
type ActiveSessionDTO struct {
	ConnectionID   string    `json:"connection_id"`
	UserID         string    `json:"user_id"`
	NodeID         string    `json:"node_id"`
	SeverityFilter string    `json:"severity_filter,omitempty"`
	ConnectedAt    time.Time `json:"connected_at"`
	BufferedEvents int       `json:"buffered_events"`
}

// ActiveSessionsResponse is the paginated response for GET /v1/notifications/sessions.
type ActiveSessionsResponse struct {
	TenantID          string             `json:"tenant_id"`
	ActiveConnections int                `json:"active_connections"`
	NextCursor        string             `json:"next_cursor,omitempty"`
	Sessions          []ActiveSessionDTO `json:"sessions"`
}

// ListActiveSessionsQuery encapsulates cursor-paginated filter parameters.
type ListActiveSessionsQuery struct {
	TenantID string
	Limit    int
	Cursor   string
}

// ListActiveSessionsHandler executes active session queries against the local ConnectionHub.
type ListActiveSessionsHandler struct {
	hub *hub.ConnectionHub
}

// NewListActiveSessionsHandler creates a ListActiveSessionsHandler.
func NewListActiveSessionsHandler(hub *hub.ConnectionHub) *ListActiveSessionsHandler {
	return &ListActiveSessionsHandler{hub: hub}
}

// Handle executes the query and returns the paginated response.
func (h *ListActiveSessionsHandler) Handle(_ context.Context, q ListActiveSessionsQuery) (ActiveSessionsResponse, error) {
	if h.hub == nil {
		return ActiveSessionsResponse{}, errors.New("connection hub cannot be nil")
	}
	if q.TenantID == "" {
		return ActiveSessionsResponse{}, errors.New("tenant_id is required")
	}

	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}

	conns, nextCursor, total := h.hub.ListActiveSessions(q.TenantID, limit, q.Cursor)

	dtos := make([]ActiveSessionDTO, 0, len(conns))
	for _, c := range conns {
		dtos = append(dtos, ActiveSessionDTO{
			ConnectionID:   c.ConnectionID,
			UserID:         c.UserID,
			NodeID:         c.NodeID,
			SeverityFilter: string(c.SeverityFilter),
			ConnectedAt:    c.ConnectedAt.UTC(),
			BufferedEvents: c.BufferedEvents,
		})
	}

	return ActiveSessionsResponse{
		TenantID:          q.TenantID,
		ActiveConnections: total,
		NextCursor:        nextCursor,
		Sessions:          dtos,
	}, nil
}
