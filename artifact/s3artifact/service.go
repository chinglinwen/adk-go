// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package s3artifact provides an Amazon S3 [artifact.Service] using Go Cloud Development Kit (CDK).
//
// This package allows storing and retrieving artifacts in an S3 bucket.
// Artifacts are organized by application name, user ID, session ID, and filename,
// with support for versioning.
package s3artifact

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"
	"gocloud.dev/gcerrors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/genai"

	"google.golang.org/adk/artifact"
)

// s3Service is an S3 implementation of the Service using gocloud.dev/blob.
type s3Service struct {
	bucket *blob.Bucket
}

// NewService creates an S3 service for the specified bucket.
func NewService(ctx context.Context, bucketName string, optFns ...func(*config.LoadOptions) error) (artifact.Service, error) {
	cfg, err := config.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("failed to load aws config: %w", err)
	}
	client := s3.NewFromConfig(cfg)

	bucket, err := s3blob.OpenBucketV2(ctx, client, bucketName, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open s3 bucket: %w", err)
	}

	s := &s3Service{
		bucket: bucket,
	}
	return s, nil
}

// fileHasUserNamespace checks if a filename indicates a user-namespaced blob.
func fileHasUserNamespace(filename string) bool {
	return strings.HasPrefix(filename, "user:")
}

// buildKey constructs the key in S3.
func buildKey(appName, userID, sessionID, fileName string, version int64) string {
	if fileHasUserNamespace(fileName) {
		return fmt.Sprintf("%s/%s/user/%s/%d", appName, userID, fileName, version)
	}
	return fmt.Sprintf("%s/%s/%s/%s/%d", appName, userID, sessionID, fileName, version)
}

func buildKeyPrefix(appName, userID, sessionID, fileName string) string {
	if fileHasUserNamespace(fileName) {
		return fmt.Sprintf("%s/%s/user/%s/", appName, userID, fileName)
	}
	return fmt.Sprintf("%s/%s/%s/%s/", appName, userID, sessionID, fileName)
}

func buildSessionPrefix(appName, userID, sessionID string) string {
	return fmt.Sprintf("%s/%s/%s/", appName, userID, sessionID)
}

func buildUserPrefix(appName, userID string) string {
	return fmt.Sprintf("%s/%s/user/", appName, userID)
}

// Save implements [artifact.Service]
func (s *s3Service) Save(ctx context.Context, req *artifact.SaveRequest) (_ *artifact.SaveResponse, err error) {
	err = req.Validate()
	if err != nil {
		return nil, fmt.Errorf("request validation failed: %w", err)
	}
	appName, userID, sessionID, fileName := req.AppName, req.UserID, req.SessionID, req.FileName
	newArtifact := req.Part

	nextVersion := int64(1)

	// TODO race condition
	response, err := s.versions(ctx, &artifact.VersionsRequest{
		AppName: req.AppName, UserID: req.UserID, SessionID: req.SessionID, FileName: req.FileName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list artifact versions: %w", err)
	}
	if len(response.Versions) > 0 {
		nextVersion = slices.Max(response.Versions) + 1
	}

	key := buildKey(appName, userID, sessionID, fileName, nextVersion)

	var opts *blob.WriterOptions
	if newArtifact.InlineData != nil {
		opts = &blob.WriterOptions{ContentType: newArtifact.InlineData.MIMEType}
		w, err := s.bucket.NewWriter(ctx, key, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create writer: %w", err)
		}
		if _, err := w.Write(newArtifact.InlineData.Data); err != nil {
			w.Close() // Best effort close
			return nil, fmt.Errorf("failed to write data: %w", err)
		}
		if err := w.Close(); err != nil {
			return nil, fmt.Errorf("failed to close writer: %w", err)
		}
	} else {
		opts = &blob.WriterOptions{ContentType: "text/plain"}
		w, err := s.bucket.NewWriter(ctx, key, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create writer: %w", err)
		}
		if _, err := w.Write([]byte(newArtifact.Text)); err != nil {
			w.Close()
			return nil, fmt.Errorf("failed to write text: %w", err)
		}
		if err := w.Close(); err != nil {
			return nil, fmt.Errorf("failed to close writer: %w", err)
		}
	}

	return &artifact.SaveResponse{Version: nextVersion}, nil
}

// Delete implements [artifact.Service]
func (s *s3Service) Delete(ctx context.Context, req *artifact.DeleteRequest) error {
	err := req.Validate()
	if err != nil {
		return fmt.Errorf("request validation failed: %w", err)
	}
	appName, userID, sessionID, fileName := req.AppName, req.UserID, req.SessionID, req.FileName
	version := req.Version

	// Delete specific version
	if version != 0 {
		key := buildKey(appName, userID, sessionID, fileName, version)
		if err := s.bucket.Delete(ctx, key); err != nil {
			if gcerrors.Code(err) == gcerrors.NotFound {
				// Deleting non-existing entry is not an error
				return nil
			}
			return fmt.Errorf("failed to delete artifact: %w", err)
		}
		return nil
	}

	// Delete all versions
	response, err := s.versions(ctx, &artifact.VersionsRequest{
		AppName: req.AppName, UserID: req.UserID, SessionID: req.SessionID, FileName: req.FileName,
	})
	if err != nil {
		return fmt.Errorf("failed to fetch versions on delete artifact: %w", err)
	}

	g, gctx := errgroup.WithContext(ctx)

	// delete versions in parallel
	for _, version := range response.Versions {
		v := version // capture loop variable for goroutine

		g.Go(func() error {
			key := buildKey(appName, userID, sessionID, fileName, v)
			if err := s.bucket.Delete(gctx, key); err != nil {
				if gcerrors.Code(err) == gcerrors.NotFound {
					return nil
				}
				return fmt.Errorf("failed to delete artifact %s: %w", key, err)
			}
			return nil
		})
	}

	return g.Wait()
}

// Load implements [artifact.Service]
func (s *s3Service) Load(ctx context.Context, req *artifact.LoadRequest) (_ *artifact.LoadResponse, err error) {
	err = req.Validate()
	if err != nil {
		return nil, fmt.Errorf("request validation failed: %w", err)
	}
	appName, userID, sessionID, fileName := req.AppName, req.UserID, req.SessionID, req.FileName
	version := req.Version

	if version == 0 {
		response, err := s.versions(ctx, &artifact.VersionsRequest{
			AppName: req.AppName, UserID: req.UserID, SessionID: req.SessionID, FileName: req.FileName,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to list artifact versions: %w", err)
		}
		if len(response.Versions) == 0 {
			return nil, fmt.Errorf("artifact not found: %w", fs.ErrNotExist)
		}
		version = slices.Max(response.Versions)
	}

	key := buildKey(appName, userID, sessionID, fileName, version)

	reader, err := s.bucket.NewReader(ctx, key, nil)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil, fmt.Errorf("artifact '%s' not found: %w", key, fs.ErrNotExist)
		}
		return nil, fmt.Errorf("could not get object '%s': %w", key, err)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("failed to close object reader: %w", closeErr)
		}
	}()

	// Read all the content into a byte slice
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("could not read data from object '%s': %w", key, err)
	}

	// Create the genai.Part and return the response.
	part := genai.NewPartFromBytes(data, reader.ContentType())

	return &artifact.LoadResponse{Part: part}, nil
}

// fetchFilenamesFromPrefix is a reusable helper function.
func (s *s3Service) fetchFilenamesFromPrefix(ctx context.Context, prefix string, filenamesSet map[string]bool) error {
	if filenamesSet == nil {
		return fmt.Errorf("filenamesSet cannot be nil")
	}

	iter := s.bucket.List(&blob.ListOptions{
		Prefix: prefix,
	})

	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error iterating objects: %w", err)
		}

		segments := strings.Split(obj.Key, "/")
		if len(segments) < 2 {
			return fmt.Errorf("error iterating objects: incorrect number of segments in path %q", obj.Key)
		}
		// appName/userId/sessionId/filename/version or appName/userId/user/filename/version
		filename := segments[len(segments)-2]
		filenamesSet[filename] = true
	}

	return nil
}

// List implements [artifact.Service]
func (s *s3Service) List(ctx context.Context, req *artifact.ListRequest) (*artifact.ListResponse, error) {
	err := req.Validate()
	if err != nil {
		return nil, fmt.Errorf("request validation failed: %w", err)
	}
	appName, userID, sessionID := req.AppName, req.UserID, req.SessionID
	filenamesSet := map[string]bool{}

	// Fetch filenames for the session.
	err = s.fetchFilenamesFromPrefix(ctx, buildSessionPrefix(appName, userID, sessionID), filenamesSet)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch session filenames: %w", err)
	}

	// Fetch filenames for the user.
	err = s.fetchFilenamesFromPrefix(ctx, buildUserPrefix(appName, userID), filenamesSet)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch user filenames: %w", err)
	}

	filenames := slices.Collect(maps.Keys(filenamesSet))
	sort.Strings(filenames)
	return &artifact.ListResponse{FileNames: filenames}, nil
}

// versions internal function that does not return error if versions are empty
func (s *s3Service) versions(ctx context.Context, req *artifact.VersionsRequest) (*artifact.VersionsResponse, error) {
	err := req.Validate()
	if err != nil {
		return nil, fmt.Errorf("request validation failed: %w", err)
	}
	appName, userID, sessionID, fileName := req.AppName, req.UserID, req.SessionID, req.FileName

	prefix := buildKeyPrefix(appName, userID, sessionID, fileName)
	iter := s.bucket.List(&blob.ListOptions{
		Prefix: prefix,
	})

	versions := make([]int64, 0)
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error iterating objects: %w", err)
		}

		segments := strings.Split(obj.Key, "/")
		if len(segments) < 1 {
			return nil, fmt.Errorf("error iterating objects: incorrect number of segments in path %q", obj.Key)
		}
		version, err := strconv.ParseInt(segments[len(segments)-1], 10, 64)
		// if the file version is not convertible to number, just ignore it
		if err != nil {
			continue
		}
		versions = append(versions, version)
	}
	return &artifact.VersionsResponse{Versions: versions}, nil
}

// Versions implements [artifact.Service] and returns an error if no versions are found.
func (s *s3Service) Versions(ctx context.Context, req *artifact.VersionsRequest) (*artifact.VersionsResponse, error) {
	response, err := s.versions(ctx, req)
	if err != nil {
		return nil, err
	}
	if len(response.Versions) == 0 {
		return nil, fmt.Errorf("artifact not found: %w", fs.ErrNotExist)
	}
	return response, nil
}

// Close closes the bucket connection
func (s *s3Service) Close() error {
	return s.bucket.Close()
}
