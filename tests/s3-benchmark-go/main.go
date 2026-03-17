package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// ── Config ──

const (
	bucketName     = "s3-bench"
	minSize        = 10 * 1024  // 10 KB
	maxSize        = 250 * 1024 // 250 KB
	totalFiles     = 5000
	numWorkers     = 100
	mpPartSize     = 5 * 1024 * 1024 // 5 MB per part
	mpTotalFiles   = 200              // fewer files for multipart (larger)
	mpMinSize      = 10 * 1024 * 1024 // 10 MB
	mpMaxSize      = 25 * 1024 * 1024 // 25 MB
	mpWorkers      = 20               // concurrent multipart uploads
)

// ── File descriptor ──

type benchFile struct {
	key      string
	data     []byte
	checksum string // MD5 hex
}

// ── Stats ──

type phaseStats struct {
	name       string
	totalOps   int64
	success    int64
	fail       int64
	totalBytes int64
	totalMs    float64
	wallMs     float64
	latencies  []float64
	mu         sync.Mutex
}

func newStats(name string) *phaseStats {
	return &phaseStats{name: name, latencies: make([]float64, 0, totalFiles)}
}

func (s *phaseStats) record(ok bool, ms float64, nbytes int64) {
	atomic.AddInt64(&s.totalOps, 1)
	if ok {
		atomic.AddInt64(&s.success, 1)
		atomic.AddInt64(&s.totalBytes, nbytes)
		s.mu.Lock()
		s.totalMs += ms
		s.latencies = append(s.latencies, ms)
		s.mu.Unlock()
	} else {
		atomic.AddInt64(&s.fail, 1)
	}
}

func (s *phaseStats) percentile(p float64) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.latencies) == 0 {
		return 0
	}
	sorted := make([]float64, len(s.latencies))
	copy(sorted, s.latencies)
	sort.Float64s(sorted)
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func (s *phaseStats) avgLatency() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.latencies) == 0 {
		return 0
	}
	return s.totalMs / float64(len(s.latencies))
}

func (s *phaseStats) opsPerSec() float64 {
	if s.wallMs <= 0 {
		return 0
	}
	return float64(s.success) / (s.wallMs / 1000.0)
}

func (s *phaseStats) mbPerSec() float64 {
	if s.wallMs <= 0 || s.totalBytes <= 0 {
		return 0
	}
	return float64(s.totalBytes) / (1024 * 1024) / (s.wallMs / 1000.0)
}

// ── Target ──

type target struct {
	name     string
	endpoint string
	ak       string
	sk       string
	client   *s3.Client
}

func newTarget(name, endpoint, ak, sk string) *target {
	cfg, _ := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, sk, "")),
	)
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	return &target{name: name, endpoint: endpoint, ak: ak, sk: sk, client: client}
}

// ── Benchmark runner for a single target ──

type benchResult struct {
	upload    *phaseStats
	download  *phaseStats
	head      *phaseStats
	multipart *phaseStats
	list      listResult
	del       *phaseStats
	mismatch  int64
}

type listResult struct {
	count int
	ms    float64
	err   error
}

func runBenchmark(t *target, files []benchFile, mpFiles []benchFile, totalDataBytes int64) *benchResult {
	ctx := context.Background()
	res := &benchResult{}

	// Create bucket
	t.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})

	// ── Upload (PUT) ──
	res.upload = newStats("Upload")
	wallStart := time.Now()
	runConcurrent(files, numWorkers, func(f *benchFile) {
		t0 := time.Now()
		_, err := t.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucketName),
			Key:         aws.String(f.key),
			Body:        bytes.NewReader(f.data),
			ContentType: aws.String("application/octet-stream"),
		})
		ms := float64(time.Since(t0).Microseconds()) / 1000.0
		res.upload.record(err == nil, ms, int64(len(f.data)))
	})
	res.upload.wallMs = float64(time.Since(wallStart).Microseconds()) / 1000.0

	// ── Download ──
	res.download = newStats("Download")
	wallStart = time.Now()
	runConcurrent(files, numWorkers, func(f *benchFile) {
		t0 := time.Now()
		out, err := t.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(f.key),
		})
		if err != nil {
			ms := float64(time.Since(t0).Microseconds()) / 1000.0
			res.download.record(false, ms, 0)
			return
		}
		body, err := io.ReadAll(out.Body)
		out.Body.Close()
		ms := float64(time.Since(t0).Microseconds()) / 1000.0
		if err != nil {
			res.download.record(false, ms, 0)
			return
		}
		res.download.record(true, ms, int64(len(body)))
		h := md5.Sum(body)
		if hex.EncodeToString(h[:]) != f.checksum {
			atomic.AddInt64(&res.mismatch, 1)
		}
	})
	res.download.wallMs = float64(time.Since(wallStart).Microseconds()) / 1000.0

	// ── HEAD ──
	res.head = newStats("HEAD")
	wallStart = time.Now()
	runConcurrent(files, numWorkers, func(f *benchFile) {
		t0 := time.Now()
		_, err := t.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(f.key),
		})
		ms := float64(time.Since(t0).Microseconds()) / 1000.0
		res.head.record(err == nil, ms, 0)
	})
	res.head.wallMs = float64(time.Since(wallStart).Microseconds()) / 1000.0

	// ── List ──
	t0 := time.Now()
	out, err := t.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	ms := float64(time.Since(t0).Microseconds()) / 1000.0
	count := 0
	if out != nil {
		count = len(out.Contents)
	}
	res.list = listResult{count: count, ms: ms, err: err}

	// ── Delete (cleanup PUT objects) ──
	res.del = newStats("Delete")
	wallStart = time.Now()
	runConcurrent(files, numWorkers, func(f *benchFile) {
		t0 := time.Now()
		_, err := t.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(f.key),
		})
		ms := float64(time.Since(t0).Microseconds()) / 1000.0
		res.del.record(err == nil, ms, 0)
	})
	res.del.wallMs = float64(time.Since(wallStart).Microseconds()) / 1000.0

	// ── Multipart Upload ──
	res.multipart = newStats("Multipart")
	wallStart = time.Now()
	runConcurrent(mpFiles, mpWorkers, func(f *benchFile) {
		t0 := time.Now()
		ok := doMultipartUpload(ctx, t.client, f)
		ms := float64(time.Since(t0).Microseconds()) / 1000.0
		res.multipart.record(ok, ms, int64(len(f.data)))
	})
	res.multipart.wallMs = float64(time.Since(wallStart).Microseconds()) / 1000.0

	// Verify multipart objects
	for i := range mpFiles {
		f := &mpFiles[i]
		out, err := t.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(f.key),
		})
		if err != nil {
			atomic.AddInt64(&res.mismatch, 1)
			continue
		}
		body, _ := io.ReadAll(out.Body)
		out.Body.Close()
		h := md5.Sum(body)
		if hex.EncodeToString(h[:]) != f.checksum {
			atomic.AddInt64(&res.mismatch, 1)
		}
	}

	// Cleanup multipart objects
	for i := range mpFiles {
		t.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(mpFiles[i].key),
		})
	}

	// Cleanup bucket
	t.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucketName)})

	return res
}

func doMultipartUpload(ctx context.Context, client *s3.Client, f *benchFile) bool {
	// Initiate
	mpu, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(f.key),
		ContentType: aws.String("application/octet-stream"),
	})
	if err != nil {
		return false
	}
	uploadId := mpu.UploadId

	// Upload parts
	var completedParts []types.CompletedPart
	partNum := int32(1)
	for offset := 0; offset < len(f.data); offset += mpPartSize {
		end := offset + mpPartSize
		if end > len(f.data) {
			end = len(f.data)
		}
		chunk := f.data[offset:end]

		resp, err := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(f.key),
			UploadId:   uploadId,
			PartNumber: aws.Int32(partNum),
			Body:       bytes.NewReader(chunk),
		})
		if err != nil {
			client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket: aws.String(bucketName), Key: aws.String(f.key), UploadId: uploadId,
			})
			return false
		}
		completedParts = append(completedParts, types.CompletedPart{
			PartNumber: aws.Int32(partNum),
			ETag:       resp.ETag,
		})
		partNum++
	}

	// Complete
	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(f.key),
		UploadId: uploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	return err == nil
}

func runConcurrent(files []benchFile, workers int, fn func(*benchFile)) {
	ch := make(chan int, len(files))
	for i := range files {
		ch <- i
	}
	close(ch)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range ch {
				fn(&files[idx])
			}
		}()
	}
	wg.Wait()
}

// ── Main ──

func main() {
	oxidbEndpoint := flag.String("oxidb", "http://127.0.0.1:9000", "OxiDB S3 endpoint")
	minioEndpoint := flag.String("minio", "http://127.0.0.1:9100", "MinIO S3 endpoint")
	ak := flag.String("ak", "benchkey", "Access key (shared)")
	sk := flag.String("sk", "benchsecret123", "Secret key (shared)")
	flag.Parse()

	fmt.Println("╔═══════════════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║            OxiDB vs MinIO — S3 Concurrent Benchmark (Go)                     ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Printf("  OxiDB:    %s\n", *oxidbEndpoint)
	fmt.Printf("  MinIO:    %s\n", *minioEndpoint)
	fmt.Printf("  Files:    %d  (size: %d KB – %d KB random)\n", totalFiles, minSize/1024, maxSize/1024)
	fmt.Printf("  Workers:  %d concurrent goroutines\n", numWorkers)
	fmt.Printf("  Multipart:%d files (size: %d MB – %d MB, %d MB parts, %d workers)\n",
		mpTotalFiles, mpMinSize/(1024*1024), mpMaxSize/(1024*1024), mpPartSize/(1024*1024), mpWorkers)
	fmt.Println()

	// ── Generate regular files ──
	fmt.Print("  Generating files... ")
	t0 := time.Now()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	files := make([]benchFile, totalFiles)
	var totalDataBytes int64

	for i := 0; i < totalFiles; i++ {
		size := minSize + rng.Intn(maxSize-minSize+1)
		data := make([]byte, size)
		rng.Read(data)
		h := md5.Sum(data)
		files[i] = benchFile{
			key:      fmt.Sprintf("bench/%05d.bin", i),
			data:     data,
			checksum: hex.EncodeToString(h[:]),
		}
		totalDataBytes += int64(size)
	}
	dataMB := float64(totalDataBytes) / (1024 * 1024)

	// ── Generate multipart files ──
	mpFiles := make([]benchFile, mpTotalFiles)
	var mpDataBytes int64
	for i := 0; i < mpTotalFiles; i++ {
		size := mpMinSize + rng.Intn(mpMaxSize-mpMinSize+1)
		data := make([]byte, size)
		rng.Read(data)
		h := md5.Sum(data)
		mpFiles[i] = benchFile{
			key:      fmt.Sprintf("mp/%05d.bin", i),
			data:     data,
			checksum: hex.EncodeToString(h[:]),
		}
		mpDataBytes += int64(size)
	}
	mpMB := float64(mpDataBytes) / (1024 * 1024)

	fmt.Printf("done (%d + %d files, %.1f + %.1f MB, %s)\n\n",
		totalFiles, mpTotalFiles, dataMB, mpMB, time.Since(t0).Round(time.Millisecond))

	// ── Create targets ──
	oxidb := newTarget("OxiDB", *oxidbEndpoint, *ak, *sk)
	minio := newTarget("MinIO", *minioEndpoint, *ak, *sk)

	// ── Verify connectivity ──
	fmt.Print("  Checking OxiDB... ")
	if _, err := oxidb.client.ListBuckets(context.Background(), &s3.ListBucketsInput{}); err != nil {
		fmt.Printf("FAILED: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("OK")

	fmt.Print("  Checking MinIO... ")
	if _, err := minio.client.ListBuckets(context.Background(), &s3.ListBucketsInput{}); err != nil {
		fmt.Printf("FAILED: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("OK")
	fmt.Println()

	// ── Run benchmarks sequentially (avoid resource contention) ──
	fmt.Println("  ┌─────────────────────────────────────────────────────────────┐")
	fmt.Println("  │  Running OxiDB benchmark...                                 │")
	fmt.Println("  └─────────────────────────────────────────────────────────────┘")
	oxidbRes := runBenchmark(oxidb, files, mpFiles, totalDataBytes)
	printTargetSummary("OxiDB", oxidbRes, totalDataBytes)

	fmt.Println()
	fmt.Println("  ┌─────────────────────────────────────────────────────────────┐")
	fmt.Println("  │  Running MinIO benchmark...                                 │")
	fmt.Println("  └─────────────────────────────────────────────────────────────┘")
	minioRes := runBenchmark(minio, files, mpFiles, totalDataBytes)
	printTargetSummary("MinIO", minioRes, totalDataBytes)

	// ── Comparison Report ──
	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║                         HEAD-TO-HEAD COMPARISON                              ║")
	fmt.Println("╠═══════════════════════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  %-10s │ %10s %10s │ %10s %10s │ %7s │ %-7s ║\n",
		"Phase", "OxiDB Wall", "OxiDB Avg", "MinIO Wall", "MinIO Avg", "Ratio", "Winner")
	fmt.Println("╠═══════════════════════════════════════════════════════════════════════════════╣")

	phases := []struct {
		name string
		o, m *phaseStats
	}{
		{"Upload", oxidbRes.upload, minioRes.upload},
		{"Download", oxidbRes.download, minioRes.download},
		{"HEAD", oxidbRes.head, minioRes.head},
		{"Multipart", oxidbRes.multipart, minioRes.multipart},
		{"Delete", oxidbRes.del, minioRes.del},
	}

	oxidbWins := 0
	minioWins := 0
	ties := 0

	for _, p := range phases {
		ratio := p.m.wallMs / p.o.wallMs
		var winner string
		if ratio > 1.05 {
			winner = "\033[32mOxiDB\033[0m"
			oxidbWins++
		} else if ratio < 0.95 {
			winner = "\033[33mMinIO\033[0m"
			minioWins++
		} else {
			winner = "  tie"
			ties++
		}
		fmt.Printf("║  %-10s │ %8.0fms %8.1fms │ %8.0fms %8.1fms │ %5.2fx │ %-7s ║\n",
			p.name,
			p.o.wallMs, p.o.avgLatency(),
			p.m.wallMs, p.m.avgLatency(),
			ratio, winner)
	}

	// List row
	listRatio := minioRes.list.ms / oxidbRes.list.ms
	var listWinner string
	if listRatio > 1.05 {
		listWinner = "\033[32mOxiDB\033[0m"
		oxidbWins++
	} else if listRatio < 0.95 {
		listWinner = "\033[33mMinIO\033[0m"
		minioWins++
	} else {
		listWinner = "  tie"
		ties++
	}
	fmt.Printf("║  %-10s │ %8.1fms %10s │ %8.1fms %10s │ %5.2fx │ %-7s ║\n",
		"List", oxidbRes.list.ms, "", minioRes.list.ms, "", listRatio, listWinner)

	fmt.Println("╠═══════════════════════════════════════════════════════════════════════════════╣")

	// Totals
	oxidbTotal := oxidbRes.upload.wallMs + oxidbRes.download.wallMs + oxidbRes.head.wallMs + oxidbRes.del.wallMs + oxidbRes.multipart.wallMs
	minioTotal := minioRes.upload.wallMs + minioRes.download.wallMs + minioRes.head.wallMs + minioRes.del.wallMs + minioRes.multipart.wallMs
	totalRatio := minioTotal / oxidbTotal

	fmt.Printf("║  %-10s │ %8.0fms %10s │ %8.0fms %10s │ %5.2fx │         ║\n",
		"TOTAL", oxidbTotal, "", minioTotal, "", totalRatio)

	fmt.Println("╠═══════════════════════════════════════════════════════════════════════════════╣")

	// Throughput comparison
	fmt.Printf("║  Upload throughput:      OxiDB %7.0f MB/s  vs  MinIO %7.0f MB/s         ║\n",
		oxidbRes.upload.mbPerSec(), minioRes.upload.mbPerSec())
	fmt.Printf("║  Download throughput:    OxiDB %7.0f MB/s  vs  MinIO %7.0f MB/s         ║\n",
		oxidbRes.download.mbPerSec(), minioRes.download.mbPerSec())
	fmt.Printf("║  Multipart throughput:   OxiDB %7.0f MB/s  vs  MinIO %7.0f MB/s         ║\n",
		oxidbRes.multipart.mbPerSec(), minioRes.multipart.mbPerSec())

	fmt.Println("╠═══════════════════════════════════════════════════════════════════════════════╣")

	// Integrity
	oInteg := "PASS"
	mInteg := "PASS"
	if oxidbRes.mismatch > 0 {
		oInteg = fmt.Sprintf("FAIL(%d)", oxidbRes.mismatch)
	}
	if minioRes.mismatch > 0 {
		mInteg = fmt.Sprintf("FAIL(%d)", minioRes.mismatch)
	}
	fmt.Printf("║  Data integrity:  OxiDB: %-8s  MinIO: %-8s                            ║\n", oInteg, mInteg)

	fmt.Println("╠═══════════════════════════════════════════════════════════════════════════════╣")

	// Scoreboard
	scoreStr := fmt.Sprintf("OxiDB %d — MinIO %d — Ties %d", oxidbWins, minioWins, ties)
	pad := 77 - 4 - len(scoreStr)
	if pad < 0 {
		pad = 0
	}
	fmt.Printf("║  %s%s║\n", scoreStr, strings.Repeat(" ", pad))

	if totalRatio > 1.0 {
		msg := fmt.Sprintf("OxiDB is %.2fx faster overall", totalRatio)
		pad = 77 - 4 - len(msg)
		if pad < 0 {
			pad = 0
		}
		fmt.Printf("║  \033[32m%s\033[0m%s║\n", msg, strings.Repeat(" ", pad))
	} else if totalRatio < 1.0 {
		msg := fmt.Sprintf("MinIO is %.2fx faster overall", 1.0/totalRatio)
		pad = 77 - 4 - len(msg)
		if pad < 0 {
			pad = 0
		}
		fmt.Printf("║  \033[33m%s\033[0m%s║\n", msg, strings.Repeat(" ", pad))
	}

	fmt.Println("╚═══════════════════════════════════════════════════════════════════════════════╝")
}

func printTargetSummary(name string, res *benchResult, totalDataBytes int64) {
	fmt.Printf("\n  %s results:\n", name)
	for _, s := range []*phaseStats{res.upload, res.download, res.head, res.multipart, res.del} {
		var tp string
		if s.totalBytes > 0 {
			tp = fmt.Sprintf("%.0f MB/s", s.mbPerSec())
		} else {
			tp = fmt.Sprintf("%.0f op/s", s.opsPerSec())
		}
		fmt.Printf("    %-10s  %5d/%d  wall:%7.0fms  avg:%6.1fms  p50:%6.1fms  p99:%6.1fms  %s\n",
			s.name, s.success, s.totalOps, s.wallMs, s.avgLatency(),
			s.percentile(0.50), s.percentile(0.99), tp)
	}
	fmt.Printf("    %-10s  listed %d objects in %.1f ms\n", "List", res.list.count, res.list.ms)
	if res.mismatch > 0 {
		fmt.Printf("    !! INTEGRITY: %d files MISMATCHED\n", res.mismatch)
	} else {
		fmt.Printf("    Integrity: ALL %d files verified (MD5)\n", res.download.success+res.multipart.success)
	}
}
