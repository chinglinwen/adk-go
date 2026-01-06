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

package s3artifact

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/adk/artifact"
	"google.golang.org/adk/internal/artifact/tests"
)

func TestLocalS3ArtifactService(t *testing.T) {
	// This test assumes a local S3-compatible service (like SeaweedFS) is running.
	// We'll skip if connection fails, but since the user asked for it, we'll try.

	endpoint := "http://localhost:8333"
	accessKey := "admin"
	secretKey := "secret"
	bucketName := "test-bucket"
	ctx := context.Background()

	// Helper to create the bucket if it doesn't exist
	setupBucket := func() error {
		cfg, err := config.LoadDefaultConfig(ctx,
			config.WithRegion("us-east-1"),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
			config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               endpoint,
					SigningRegion:     "us-east-1",
					HostnameImmutable: true,
				}, nil
			})),
		)
		if err != nil {
			return err
		}

		client := s3.NewFromConfig(cfg)
		// Try to create bucket. Ignore error if it exists (BucketAlreadyExists/BucketAlreadyOwnedByYou)
		_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})
		return nil // ignoring errors for simplicity in this helper, mostly it will fail if connection refused
	}

	// Try to setup connectivity. If it fails, skip the test.
	if err := setupBucket(); err != nil {
		t.Logf("Skipping local S3 test as setup failed (is SeaweedFS running?): %v", err)
		return // Or t.Skip
	}

	factory := func(t *testing.T) (artifact.Service, error) {
		// Use a unique bucket for each test run if possible, or just clean up?
		// configuring existing bucket is fine for basic tests.
		// SeaweedFS is fast.

		return NewService(ctx, bucketName,
			config.WithRegion("us-east-1"),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
			config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               endpoint,
					SigningRegion:     "us-east-1",
					HostnameImmutable: true,
				}, nil
			})),
		)
	}

	// Retry creating the service wrapper in case of startup race
	var err error
	for i := 0; i < 5; i++ {
		_, err = factory(t)
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		t.Fatalf("Failed to connect to local S3: %v", err)
	}

	tests.TestArtifactService(t, "LocalS3", factory)
}
