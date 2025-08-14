package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type LogLevel string

const (
	LogLevelDebug LogLevel = "debug"
	LogLevelInfo  LogLevel = "info" 
	LogLevelWarn  LogLevel = "warn"
	LogLevelError LogLevel = "error"
	LogLevelFatal LogLevel = "fatal"
)

type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Stack   string `json:"stack,omitempty"`
}

type Log struct {
	Type        string                 `json:"type"`
	Timestamp   time.Time              `json:"timestamp"`
	Level       LogLevel               `json:"level"`
	Service     string                 `json:"service"`
	TraceID     string                 `json:"trace_id,omitempty"`
	SpanID      string                 `json:"span_id,omitempty"`
	Message     string                 `json:"message"`
	ErrorID     *int                   `json:"error_id,omitempty"`
	ErrorDetail *ErrorDetail           `json:"error_detail,omitempty"`
	Context     map[string]interface{} `json:"context,omitempty"`
	Duration    *time.Duration         `json:"duration,omitempty"`
	
	// Additional fields from FluentBit/Kubernetes
	Kubernetes map[string]interface{} `json:"kubernetes,omitempty"`
	Raw        map[string]interface{} `json:"-"`
}

type Server struct {
	client    *mongo.Client
	apiKey    string
	defaultDB string
	defaultCollection string
}

func NewServer() (*Server, error) {
	mongoURI := os.Getenv("MONGODB_URI")
	if mongoURI == "" {
		return nil, fmt.Errorf("MONGODB_URI environment variable required")
	}

	apiKey := os.Getenv("API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("API_KEY environment variable required")
	}

	defaultDB := os.Getenv("MONGODB_DB")
	if defaultDB == "" {
		defaultDB = "logging"
	}

	defaultCollection := os.Getenv("MONGODB_COLLECTION") 
	if defaultCollection == "" {
		defaultCollection = "logs"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Test connection
	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	log.Printf("Connected to MongoDB successfully")

	return &Server{
		client:            client,
		apiKey:            apiKey,
		defaultDB:         defaultDB,
		defaultCollection: defaultCollection,
	}, nil
}

func (s *Server) authenticate(r *http.Request) bool {
	apiKey := r.Header.Get("X-API-Key")
	return apiKey == s.apiKey
}

func (s *Server) ingestHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !s.authenticate(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Get database and collection from query params
	db := r.URL.Query().Get("db")
	if db == "" {
		db = s.defaultDB
	}

	collection := r.URL.Query().Get("collection") 
	if collection == "" {
		collection = s.defaultCollection
	}

	// Parse incoming JSON
	var rawData interface{}
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&rawData); err != nil {
		log.Printf("Failed to decode JSON: %v", err)
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Handle both single log and array of logs
	var logs []interface{}
	switch data := rawData.(type) {
	case []interface{}:
		logs = data
	case map[string]interface{}:
		logs = []interface{}{data}
	default:
		log.Printf("Unexpected data format: %T", rawData)
		http.Error(w, "Invalid data format", http.StatusBadRequest)
		return
	}

	// Process each log entry
	var documents []interface{}
	for _, logEntry := range logs {
		logMap, ok := logEntry.(map[string]interface{})
		if !ok {
			continue
		}

		// Transform FluentBit log to our structured format
		processedLog := s.transformLog(logMap)
		documents = append(documents, processedLog)
	}

	if len(documents) == 0 {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Insert into MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	coll := s.client.Database(db).Collection(collection)
	
	if len(documents) == 1 {
		_, err := coll.InsertOne(ctx, documents[0])
		if err != nil {
			log.Printf("Failed to insert log: %v", err)
			http.Error(w, "Database error", http.StatusInternalServerError)
			return
		}
	} else {
		_, err := coll.InsertMany(ctx, documents)
		if err != nil {
			log.Printf("Failed to insert logs: %v", err)
			http.Error(w, "Database error", http.StatusInternalServerError)
			return
		}
	}

	log.Printf("Inserted %d logs into %s.%s", len(documents), db, collection)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) transformLog(logMap map[string]interface{}) map[string]interface{} {
	// Start with the original log data
	processed := make(map[string]interface{})
	
	// Copy all original fields
	for k, v := range logMap {
		processed[k] = v
	}

	// Parse timestamp
	if timeStr, ok := logMap["@timestamp"].(string); ok {
		if ts, err := time.Parse(time.RFC3339Nano, timeStr); err == nil {
			processed["timestamp"] = ts
		}
	} else {
		processed["timestamp"] = time.Now()
	}

	// Extract service name from Kubernetes metadata
	if k8s, ok := logMap["kubernetes"].(map[string]interface{}); ok {
		if containerName, ok := k8s["container_name"].(string); ok {
			processed["service"] = containerName
		}
		if namespace, ok := k8s["namespace_name"].(string); ok {
			processed["namespace"] = namespace
		}
	}

	// Parse log message if it's JSON
	if logStr, ok := logMap["log"].(string); ok {
		logStr = strings.TrimSpace(logStr)
		if strings.HasPrefix(logStr, "{") && strings.HasSuffix(logStr, "}") {
			var jsonLog map[string]interface{}
			if err := json.Unmarshal([]byte(logStr), &jsonLog); err == nil {
				// Merge JSON log fields into processed log
				for k, v := range jsonLog {
					processed[k] = v
				}
			} else {
				processed["message"] = logStr
			}
		} else {
			processed["message"] = logStr
		}
	}

	// Set default type if not present
	if _, ok := processed["type"]; !ok {
		processed["type"] = "log"
	}

	// Parse level if present
	if levelStr, ok := processed["level"].(string); ok {
		processed["level"] = strings.ToLower(levelStr)
	}

	return processed
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := s.client.Ping(ctx, nil)
	if err != nil {
		log.Printf("Health check failed: %v", err)
		http.Error(w, "Database unhealthy", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "healthy",
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

func main() {
	server, err := NewServer()
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	http.HandleFunc("/ingest", server.ingestHandler)
	http.HandleFunc("/healthz", server.healthHandler)
	http.HandleFunc("/health", server.healthHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Starting fluent-bit-bridge server on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}