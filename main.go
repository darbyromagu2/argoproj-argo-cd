package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// Secret represents a Kubernetes Secret containing repository credentials.
type Secret struct {
	Name            string
	Data            map[string]string
	ResourceVersion string
}

// Repository represents a Git repository configuration in Argo CD.
type Repository struct {
	URL        string
	SecretName string
}

// GitClient represents a Git client connection.
type GitClient struct {
	URL             string
	Credentials     string
	ResourceVersion string
	Closed          bool
}

// FetchCommits simulates fetching commits from a remote Git repository.
func (c *GitClient) FetchCommits() ([]string, error) {
	if c.Closed {
		return nil, errors.New("git client is closed")
	}
	if c.Credentials == "expired" {
		return nil, errors.New("authentication failed: 401 Unauthorized")
	}
	if c.Credentials == "temp_error" {
		return nil, errors.New("temporary connection failure: SSH handshake failed")
	}
	return []string{"commit_latest"}, nil
}

// Close closes the Git client connection.
func (c *GitClient) Close() {
	c.Closed = true
}

// DB simulates the Argo CD database/Kubernetes API client.
type DB struct {
	mu        sync.RWMutex
	secrets   map[string]*Secret
	repos     map[string]*Repository
}

func NewDB() *DB {
	return &DB{
		secrets: make(map[string]*Secret),
		repos:   make(map[string]*Repository),
	}
}

func (db *DB) SetSecret(secret *Secret) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.secrets[secret.Name] = secret
}

func (db *DB) GetSecret(name string) (*Secret, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	secret, exists := db.secrets[name]
	if !exists {
		return nil, errors.New("secret not found")
	}
	return secret, nil
}

func (db *DB) SetRepository(repo *Repository) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.repos[repo.URL] = repo
}

func (db *DB) GetRepository(url string) (*Repository, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	repo, exists := db.repos[url]
	if !exists {
		return nil, errors.New("repository not found")
	}
	return repo, nil
}

// GitClientCache manages cached Git clients.
type GitClientCache struct {
	mu      sync.RWMutex
	clients map[string]*GitClient
	db      *DB
}

func NewGitClientCache(db *DB) *GitClientCache {
	return &GitClientCache{
		clients: make(map[string]*GitClient),
		db:      db,
	}
}

// GetClient retrieves a Git client for the given repository.
// It dynamically checks if the underlying secret has been updated.
// If the secret has been updated (ResourceVersion mismatch), it invalidates the cached client.
func (cache *GitClientCache) GetClient(repoURL string) (*GitClient, error) {
	repo, err := cache.db.GetRepository(repoURL)
	if err != nil {
		return nil, err
	}

	secret, err := cache.db.GetSecret(repo.SecretName)
	if err != nil {
		return nil, err
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	cachedClient, exists := cache.clients[repoURL]
	if exists {
		// Check if the credentials have been rotated/updated
		if cachedClient.ResourceVersion != secret.ResourceVersion {
			// Invalidate and close the stale client immediately
			cachedClient.Close()
			delete(cache.clients, repoURL)
		} else {
			return cachedClient, nil
		}
	}

	// Create a new client with the updated credentials
	newClient := &GitClient{
		URL:             repoURL,
		Credentials:     secret.Data["token"],
		ResourceVersion: secret.ResourceVersion,
	}
	cache.clients[repoURL] = newClient
	return newClient, nil
}

// InvalidateOnError invalidates the cached client if an authentication error occurs.
// This ensures temporary authentication failures do not result in a permanently cached error state.
func (cache *GitClientCache) InvalidateOnError(repoURL string, err error) {
	if err == nil {
		return
	}
	// If it's an authentication or connection error, invalidate the client
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if client, exists := cache.clients[repoURL]; exists {
		client.Close()
		delete(cache.clients, repoURL)
	}
}

func main() {
	db := NewDB()
	cache := NewGitClientCache(db)

	// Setup initial repository and secret
	secretName := "repo-creds"
	repoURL := "https://github.com/example/repo.git"

	db.SetSecret(&Secret{
		Name:            secretName,
		ResourceVersion: "1",
		Data:            map[string]string{"token": "token_v1"},
	})

	db.SetRepository(&Repository{
		URL:        repoURL,
		SecretName: secretName,
	})

	// 1. Initial fetch
	client1, err := cache.GetClient(repoURL)
	if err != nil {
		panic(fmt.Sprintf("Failed to get client: %v", err))
	}
	commits, err := client1.FetchCommits()
	if err != nil {
		panic(fmt.Sprintf("Failed to fetch commits: %v", err))
	}
	fmt.Printf("Successfully fetched commits: %v\n", commits)

	// 2. Rotate credentials
	fmt.Println("Rotating credentials to token_v2...")
	db.SetSecret(&Secret{
		Name:            secretName,
		ResourceVersion: "2",
		Data:            map[string]string{"token": "token_v2"},
	})

	// 3. Fetch again - cache must dynamically detect update and invalidate client1
	client2, err := cache.GetClient(repoURL)
	if err != nil {
		panic(fmt.Sprintf("Failed to get client after rotation: %v", err))
	}
	if client2.Credentials != "token_v2" {
		panic("Expected client to use token_v2")
	}
	if !client1.Closed {
		panic("Expected old client to be closed/invalidated")
	}
	fmt.Println("Successfully detected credential rotation and invalidated stale client.")

	// 4. Simulate temporary authentication failure during rotation window
	fmt.Println("Simulating temporary authentication failure...")
	db.SetSecret(&Secret{
		Name:            secretName,
		ResourceVersion: "3",
		Data:            map[string]string{"token": "temp_error"},
	})

	client3, err := cache.GetClient(repoURL)
	if err != nil {
		panic(err)
	}
	_, err = client3.FetchCommits()
	if err == nil {
		panic("Expected fetch to fail with temporary error")
	}
	fmt.Printf("Fetch failed as expected: %v\n", err)

	// Invalidate on error to prevent permanently cached error state
	cache.InvalidateOnError(repoURL, err)

	// 5. Recover from temporary failure with valid credentials
	fmt.Println("Recovering from temporary failure with valid credentials...")
	db.SetSecret(&Secret{
		Name:            secretName,
		ResourceVersion: "4",
		Data:            map[string]string{"token": "token_v3"},
	})

	client4, err := cache.GetClient(repoURL)
	if err != nil {
		panic(err)
	}
	commits4, err := client4.FetchCommits()
	if err != nil {
		panic(fmt.Sprintf("Failed to fetch commits after recovery: %v", err))
	}
	if client4.Credentials != "token_v3" {
		panic("Expected client to use token_v3")
	}
	fmt.Printf("Successfully recovered and fetched commits: %v\n", commits4)
	fmt.Println("All tests passed successfully!")
}