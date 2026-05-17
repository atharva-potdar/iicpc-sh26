package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"sync"
	"time"

	"github.com/google/uuid"
)

var allowedLanguages = map[string]bool{
	"cpp":  true,
	"rust": true,
	"go":   true,
}

var teamNameRe = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

const maxTeamNameLen = 64

type pendingSubmit struct {
	Language string
	TeamName string
}

type SubmitInitRequest struct {
	Language string `json:"language"`
	TeamName string `json:"team_name"`
}

type SubmitInitResponse struct {
	SubmissionID string `json:"submission_id"`
	PresignedURL string `json:"presigned_url"`
	ArtifactKey  string `json:"artifact_key"`
	ExpiresAt    int64  `json:"expires_at"`
}

type Handler struct {
	storage   *Storage
	publisher *Publisher
	maxBytes  int64
	pending   sync.Map
}

func NewHandler(storage *Storage, publisher *Publisher, maxMB int64) *Handler {
	return &Handler{
		storage:   storage,
		publisher: publisher,
		maxBytes:  maxMB * 1024 * 1024,
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Debug("writeJSON encode failed", "error", err)
	}
}

func (h *Handler) handleSubmitInit(w http.ResponseWriter, r *http.Request) {
	log := loggerFor(r)
	var req SubmitInitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}
	defer func() {
		if err := r.Body.Close(); err != nil {
			log.Debug("body close failed", "error", err)
		}
	}()
	if !allowedLanguages[req.Language] {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unsupported language"})
		return
	}
	if req.TeamName == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "team_name required"})
		return
	}
	if len(req.TeamName) > maxTeamNameLen {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "team_name too long"})
		return
	}
	if !teamNameRe.MatchString(req.TeamName) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "team_name contains invalid characters"})
		return
	}
	submissionID := uuid.New().String()
	artifactKey := fmt.Sprintf("submissions/%s.tar.gz", submissionID)
	const lifetime = 15 * time.Minute
	url, err := h.storage.PresignUpload(r.Context(), artifactKey, lifetime)
	if err != nil {
		log.Error("presign upload", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}
	h.pending.Store(submissionID, pendingSubmit{Language: req.Language, TeamName: req.TeamName})
	writeJSON(w, http.StatusAccepted, SubmitInitResponse{
		SubmissionID: submissionID,
		PresignedURL: url,
		ArtifactKey:  artifactKey,
		ExpiresAt:    time.Now().Add(lifetime).Unix(),
	})
}

func (h *Handler) handleConfirm(w http.ResponseWriter, r *http.Request) {
	log := loggerFor(r)
	submissionID := r.PathValue("id")
	if submissionID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "submission_id required"})
		return
	}
	v, ok := h.pending.Load(submissionID)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "submission not found or expired"})
		return
	}
	pending := v.(pendingSubmit)
	artifactPath := fmt.Sprintf("submissions/%s.tar.gz", submissionID)
	event := SubmissionCreatedEvent{
		SubmissionID: submissionID,
		Language:     pending.Language,
		TeamName:     pending.TeamName,
		ArtifactPath: artifactPath,
	}
	if err := h.publisher.PublishSubmissionCreated(r.Context(), event); err != nil {
		log.Error("publish event", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}
	h.pending.Delete(submissionID)
	log.Info("submission confirmed", "id", submissionID, "lang", pending.Language, "team", pending.TeamName)
	writeJSON(w, http.StatusAccepted, map[string]string{"submission_id": submissionID})
}
