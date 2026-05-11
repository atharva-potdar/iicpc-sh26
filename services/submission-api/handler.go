package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/google/uuid"
)

var allowedLanguages = map[string]bool{
	"cpp":  true,
	"rust": true,
	"go":   true,
}

type Handler struct {
	storage   *Storage
	publisher *Publisher
	maxBytes  int64
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
	json.NewEncoder(w).Encode(v)
}

func validateTarGz(data []byte) error {
	if len(data) < 2 || data[0] != 0x1f || data[1] != 0x8b {
		return fmt.Errorf("not a gzip archive")
	}
	gr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("invalid gzip: %w", err)
	}
	defer gr.Close()
	tr := tar.NewReader(gr)
	if _, err := tr.Next(); err != nil {
		return fmt.Errorf("invalid tar: %w", err)
	}
	return nil
}

func (h *Handler) handleSubmit(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, h.maxBytes)
	if err := r.ParseMultipartForm(h.maxBytes); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "file too large"})
		return
	}

	language := r.FormValue("language")
	if !allowedLanguages[language] {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unsupported language"})
		return
	}

	teamName := r.FormValue("team_name")
	if teamName == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "team_name required"})
		return
	}

	file, _, err := r.FormFile("source")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing source file"})
		return
	}
	defer file.Close()

	raw, err := io.ReadAll(file)
	if err != nil {
		log.Printf("read upload: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	if err := validateTarGz(raw); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid archive"})
		return
	}

	submissionID := uuid.New().String()
	artifactPath := fmt.Sprintf("submissions/%s.tar.gz", submissionID)

	if err := h.storage.Upload(
		r.Context(),
		artifactPath,
		bytes.NewReader(raw),
		int64(len(raw)),
	); err != nil {
		log.Printf("upload to seaweedfs: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	event := SubmissionCreatedEvent{
		SubmissionID: submissionID,
		Language:     language,
		TeamName:     teamName,
		ArtifactPath: artifactPath,
	}
	if err := h.publisher.PublishSubmissionCreated(r.Context(), event); err != nil {
		log.Printf("publish event: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	log.Printf("submission created: id=%s lang=%s team=%s", submissionID, language, teamName)
	writeJSON(w, http.StatusAccepted, map[string]string{"submission_id": submissionID})
}
