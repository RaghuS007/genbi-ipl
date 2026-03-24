package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const (
	defaultGatewayPort    = "8080"
	defaultPythonBaseURL  = "http://intelligence:8000"
	maxQueryLength        = 500
	pythonRequestTimeout  = 45 * time.Second
	shutdownGracePeriod   = 10 * time.Second
	requestIDHeader       = "X-Request-ID"
	contentTypeJSON       = "application/json"
	staticDirectory       = "/app/static"
	staticIndexPath       = "/"
	pythonHealthPath      = "/health"
	pythonQueryPath       = "/query"
	gatewayHealthPath     = "/health"
	gatewayQueryProxyPath = "/api/query"
)

type config struct {
	GatewayPort      string
	PythonServiceURL string
}

type queryRequest struct {
	Query string `json:"query"`
}

type pythonQueryRequest struct {
	Query     string `json:"query"`
	RequestID string `json:"request_id"`
}

type errorResponse struct {
	Error string `json:"error"`
}

type gatewayHealthResponse struct {
	Status        string `json:"status"`
	Service       string `json:"service"`
	PythonService string `json:"python_service"`
}

type gatewayLogger struct {
	logger *slog.Logger
}

func main() {
	cfg := loadConfig()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(requestIDMiddleware())
	router.Use(loggingMiddleware(&gatewayLogger{logger: logger}))

	httpClient := &http.Client{
		Timeout: pythonRequestTimeout,
	}

	router.GET(gatewayHealthPath, handleGatewayHealth(httpClient, cfg.PythonServiceURL))
	router.POST(gatewayQueryProxyPath, handleQueryProxy(httpClient, cfg.PythonServiceURL))
	router.StaticFile(staticIndexPath, staticDirectory+"/index.html")
	router.Static("/static", staticDirectory)

	server := &http.Server{
		Addr:              ":" + cfg.GatewayPort,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}

	logger.Info("starting gateway server",
		slog.String("port", cfg.GatewayPort),
		slog.String("python_service_url", cfg.PythonServiceURL),
	)

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("gateway server stopped unexpectedly", slog.String("error", err.Error()))
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), shutdownGracePeriod)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		logger.Error("gateway server shutdown failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func loadConfig() config {
	port := strings.TrimSpace(os.Getenv("GATEWAY_PORT"))
	if port == "" {
		port = defaultGatewayPort
	}

	pythonURL := strings.TrimSpace(os.Getenv("PYTHON_SERVICE_URL"))
	if pythonURL == "" {
		pythonURL = defaultPythonBaseURL
	}

	return config{
		GatewayPort:      port,
		PythonServiceURL: strings.TrimRight(pythonURL, "/"),
	}
}

func requestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		requestID := c.GetHeader(requestIDHeader)
		if strings.TrimSpace(requestID) == "" {
			requestID = uuid.NewString()
		}

		c.Set("request_id", requestID)
		c.Writer.Header().Set(requestIDHeader, requestID)
		c.Next()
	}
}

func loggingMiddleware(logger *gatewayLogger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		requestID := getRequestID(c)
		latency := time.Since(start)

		logger.logger.Info("request completed",
			slog.String("request_id", requestID),
			slog.String("method", c.Request.Method),
			slog.String("path", c.Request.URL.Path),
			slog.Int("status", c.Writer.Status()),
			slog.Int64("latency_ms", latency.Milliseconds()),
			slog.String("client_ip", c.ClientIP()),
		)
	}
}

func handleGatewayHealth(client *http.Client, pythonBaseURL string) gin.HandlerFunc {
	return func(c *gin.Context) {
		requestID := getRequestID(c)

		pythonStatus := "unavailable"
		if err := checkPythonHealth(c.Request.Context(), client, pythonBaseURL, requestID); err == nil {
			pythonStatus = "ok"
		}

		c.JSON(http.StatusOK, gatewayHealthResponse{
			Status:        "ok",
			Service:       "gateway",
			PythonService: pythonStatus,
		})
	}
}

func handleQueryProxy(client *http.Client, pythonBaseURL string) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req queryRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, errorResponse{
				Error: "invalid request body",
			})
			return
		}

		req.Query = strings.TrimSpace(req.Query)
		if req.Query == "" {
			c.JSON(http.StatusBadRequest, errorResponse{
				Error: "query is required",
			})
			return
		}

		if len(req.Query) > maxQueryLength {
			c.JSON(http.StatusBadRequest, errorResponse{
				Error: "query must be 500 characters or fewer",
			})
			return
		}

		payload := pythonQueryRequest{
			Query:     req.Query,
			RequestID: getRequestID(c),
		}

		responseBody, statusCode, err := forwardQuery(c.Request.Context(), client, pythonBaseURL, payload)
		if err != nil {
			c.JSON(http.StatusBadGateway, errorResponse{
				Error: "failed to reach intelligence service",
			})
			return
		}

		c.Data(statusCode, contentTypeJSON, responseBody)
	}
}

func checkPythonHealth(
	ctx context.Context,
	client *http.Client,
	pythonBaseURL string,
	requestID string,
) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pythonBaseURL+pythonHealthPath, nil)
	if err != nil {
		return err
	}

	req.Header.Set(requestIDHeader, requestID)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.New("python health check returned non-200 status")
	}

	return nil
}

func forwardQuery(
	ctx context.Context,
	client *http.Client,
	pythonBaseURL string,
	payload pythonQueryRequest,
) ([]byte, int, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, 0, err
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		pythonBaseURL+pythonQueryPath,
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, 0, err
	}

	req.Header.Set("Content-Type", contentTypeJSON)
	req.Header.Set(requestIDHeader, payload.RequestID)

	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}

	return responseBody, resp.StatusCode, nil
}

func getRequestID(c *gin.Context) string {
	requestID, exists := c.Get("request_id")
	if !exists {
		return ""
	}

	value, ok := requestID.(string)
	if !ok {
		return ""
	}

	return value
}
