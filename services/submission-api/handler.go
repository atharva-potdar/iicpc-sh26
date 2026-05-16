package main

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"

	"github.com/google/uuid"
)

var allowedLanguages = map[string]bool{
	"cpp":  true,
	"rust": true,
	"go":   true,
}

var teamNameRe = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

const maxTeamNameLen = 64

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

func validateTarGz(r io.Reader) error {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("invalid gzip: %w", err)
	}
	defer gr.Close()
	tr := tar.NewReader(gr)
	for {
		_, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("invalid tar: %w", err)
		}
	}
	return nil
}

func (h *Handler) handleSubmit(w http.ResponseWriter, r *http.Request) {
	log := loggerFor(r)

	r.Body = http.MaxBytesReader(w, r.Body, h.maxBytes)
	if err := r.ParseMultipartForm(2 << 20); err != nil {
		if _, ok := err.(*http.MaxBytesError); ok {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "file too large"})
		} else {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		}
		return
	}
	defer r.MultipartForm.RemoveAll()

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
	if len(teamName) > maxTeamNameLen {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "team_name too long"})
		return
	}
	if !teamNameRe.MatchString(teamName) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "team_name contains invalid characters"})
		return
	}

	file, _, err := r.FormFile("source")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing source file"})
		return
	}
	defer file.Close()

	if err := validateTarGz(file); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid archive"})
		return
	}

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		log.Error("seek upload", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	submissionID := uuid.New().String()
	artifactPath := fmt.Sprintf("submissions/%s.tar.gz", submissionID)

	if err := h.storage.Upload(r.Context(), artifactPath, file); err != nil {
		log.Error("upload to seaweedfs", "error", err)
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
		log.Error("publish event", "error", err)
		if delErr := h.storage.Delete(r.Context(), artifactPath); delErr != nil {
			log.Error("failed to clean up orphaned object", "path", artifactPath, "error", delErr)
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	log.Info("submission created", "id", submissionID, "lang", language, "team", teamName)
	writeJSON(w, http.StatusAccepted, map[string]string{"submission_id": submissionID})
}
