package httpapi

import (
	"WBTests/internal/Models"
	"context"
	"encoding/json"
	"net/http"
	"strings"
)

type Store interface {
	GetOrder(ctx context.Context, id string) (model.Order, error)
}

type Cache interface {
	Get(id string) (model.Order, bool)
	Set(id string, o model.Order)
}

type Handler struct {
	store Store
	cache Cache
}

func NewHandler(s Store, c Cache) *Handler {
	return &Handler{store: s, cache: c}
}

func (h *Handler) Routes(mux *http.ServeMux) {
	mux.HandleFunc("/order/", h.getOrder) // все, что начинается с /order/
}

func (h *Handler) getOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/order/")
	if id == "" {
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}

	if o, ok := h.cache.Get(id); ok {
		writeJSON(w, http.StatusOK, o)
		return
	}

	o, err := h.store.GetOrder(r.Context(), id)
	if err != nil {
		if isNotFound(err) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	h.cache.Set(id, o)
	writeJSON(w, http.StatusOK, o)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func isNotFound(err error) bool {
	return false
}
