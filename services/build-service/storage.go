package main

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// GeneratePresignedGetURL generates a pre-signed GET URL for downloading artifacts from SeaweedFS
func (b *Builder) GeneratePresignedGetURL(ctx context.Context, key string, expiry time.Duration) (string, error) {
	presignClient := s3.NewPresignClient(b.s3Client)
	req, err := presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(srcBucket),
		Key:    aws.String(key),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = expiry
	})
	if err != nil {
		return "", fmt.Errorf("presign get object %s: %w", key, err)
	}
	return req.URL, nil
}

// GeneratePresignedPutURL generates a pre-signed PUT URL for uploading binaries to SeaweedFS
func (b *Builder) GeneratePresignedPutURL(ctx context.Context, key string, expiry time.Duration) (string, error) {
	presignClient := s3.NewPresignClient(b.s3Client)
	req, err := presignClient.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(srcBucket),
		Key:    aws.String(key),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = expiry
	})
	if err != nil {
		return "", fmt.Errorf("presign put object %s: %w", key, err)
	}
	return req.URL, nil
}
