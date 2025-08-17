package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/streadway/amqp"
)

// --- Konfigurasi default ---
// Konstanta di bagian ini mengatur nilai default yang digunakan
// oleh aplikasi, seperti alamat RabbitMQ/Redis, nama antrean,
// prefix kunci di Redis, dan konfigurasi worker/producer.
// Nilai-nilai ini bisa di override lewat environment variable.
const (
	DefaultRabbitURL = "amqp://guest:guest@localhost:5672/"
	DefaultRedisURL  = "localhost:6379"
	QueueName        = "report_requests"

	KeyPrefixReportStatus = "report:status:"
	KeyPrefixReportData   = "report:data:"

	DefaultNumProducerRequests = 10
	DefaultNumWorkers          = 3
	DefaultWorkerTimeout       = 5 * time.Second
	DefaultPublishInterval     = 500 * time.Millisecond
)

// --- Struktur Data ---
// Tipe-tipe data berikut merepresentasikan payload yang dikirim
// melalui antrean (ReportRequest), status dan hasil pemrosesan
// laporan (ReportStatus dan ReportResult).
type ReportRequest struct {
	ID         string            `json:"id"`
	ReportType string            `json:"report_type"`
	Parameters map[string]string `json:"parameters"`
	CreatedAt  time.Time         `json:"created_at"`
}

type ReportStatus string

const (
	StatusPending    ReportStatus = "PENDING"
	StatusInProgress ReportStatus = "IN_PROGRESS"
	StatusCompleted  ReportStatus = "COMPLETED"
	StatusFailed     ReportStatus = "FAILED"
)

type ReportResult struct {
	RequestID   string       `json:"request_id"`
	Status      ReportStatus `json:"status"`
	GeneratedAt time.Time    `json:"generated_at"`
	ReportData  string       `json:"report_data,omitempty"`
	Error       string       `json:"error,omitempty"`
}

// --- Utilities koneksi ---
// Fungsi pembantu untuk membuka koneksi ke RabbitMQ dan Redis.
// Tujuannya agar kode utama (main / orchestrator) lebih bersih
// dan fokus pada alur aplikasi.
func connectRabbitMQ(ctx context.Context, uri string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(uri)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	_, err = ch.QueueDeclare(
		QueueName,
		true,  // durable
		false, // auto-delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, fmt.Errorf("failed to declare a queue: %w", err)
	}

	// Prefetch agar tidak membanjiri worker
	if err := ch.Qos(DefaultNumWorkers, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	log.Println("Connected to RabbitMQ and declared queue:", QueueName)
	return conn, ch, nil
}

func connectRedis(ctx context.Context, addr string) *redis.Client {
	// Buat client Redis dan cek koneksi segera.
	// Jika gagal, aplikasi dihentikan karena Redis diperlukan
	// untuk menyimpan status/report hasil pemrosesan.
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: 0})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	log.Println("Connected to Redis:", addr)
	return rdb
}

// --- Simulasi proses laporan ---
// Fungsi ini mensimulasikan pembuatan laporan yang memakan waktu
// dan kadang-kadang gagal. Dipakai untuk meniru pekerjaan nyata
// dalam contoh ini (delay acak, kemungkinan gagal 20% dari 100%).
func simulateReportGeneration(ctx context.Context, request ReportRequest) (string, error) {
	log.Printf("[Worker %s] Start generating report (type=%s)", request.ID, request.ReportType)
	delay := time.Duration(1+rand.Intn(5)) * time.Second
	select {
	case <-time.After(delay):
	case <-ctx.Done():
		log.Printf("[Worker %s] Cancelled by context", request.ID)
		return "", ctx.Err()
	}
	if rand.Intn(100) < 20 {
		return "", fmt.Errorf("simulated failure for ID %s", request.ID)
	}
	data := fmt.Sprintf("Report %s - type=%s - at=%s - data=%d",
		request.ID, request.ReportType, time.Now().Format(time.RFC3339), rand.Intn(1000))
	return data, nil
}

// --- 1) Producer ---
// Producer bertanggung jawab membuat pesan `ReportRequest` dan
// menerbitkannya ke antrean RabbitMQ. Producer bisa dijalankan
// bersamaan dengan processor agar kita dapat menguji alur end-to-end.
func produceReportRequests(ctx context.Context, wg *sync.WaitGroup, ch *amqp.Channel, num int, interval time.Duration) {
	defer wg.Done()

	t := time.NewTicker(interval)
	defer t.Stop()

	for i := 0; i < num; i++ {
		select {
		case <-ctx.Done():
			log.Println("[Producer] Context cancelled, stop producing")
			return
		case <-t.C:
			req := ReportRequest{
				ID:         fmt.Sprintf("req-%d-%d", time.Now().Unix(), i),
				ReportType: []string{"sales", "inventory", "finance"}[rand.Intn(3)],
				Parameters: map[string]string{"region": "JKT", "channel": "online"},
				CreatedAt:  time.Now(),
			}
			body, _ := json.Marshal(req)
			err := ch.Publish(
				"",        // default exchange
				QueueName, // routing key = queue
				false,     // mandatory
				false,     // immediate
				amqp.Publishing{
					ContentType:  "application/json",
					DeliveryMode: amqp.Persistent, // durable message
					Body:         body,
				},
			)
			if err != nil {
				log.Printf("[Producer] Failed to publish: %v", err)
				// continue; let next tick try again
			} else {
				log.Printf("[Producer] Published: %s", req.ID)
			}
		}
	}
	log.Println("[Producer] Finished producing messages")
}

// --- 2) Update status ke Redis ---
// Fungsi ini menyimpan status singkat dan (jika final) hasil lengkap
// pemrosesan ke Redis. Status pendek memungkinkan klien mengecek
// progres tanpa mengunduh seluruh data laporan.
func updateReportStatus(ctx context.Context, rdb *redis.Client, requestID string, status ReportStatus, reportData string, errMsg string) error {
	// Simpan status saat ini
	statusKey := KeyPrefixReportStatus + requestID
	if err := rdb.Set(ctx, statusKey, string(status), 0).Err(); err != nil {
		return fmt.Errorf("failed to set status: %w", err)
	}

	// Jika final, simpan hasil lengkap
	if status == StatusCompleted || status == StatusFailed {
		result := ReportResult{
			RequestID:   requestID,
			Status:      status,
			GeneratedAt: time.Now(),
			ReportData:  reportData,
			Error:       errMsg,
		}
		b, _ := json.Marshal(result)
		dataKey := KeyPrefixReportData + requestID
		if err := rdb.Set(ctx, dataKey, b, 0).Err(); err != nil {
			return fmt.Errorf("failed to set result: %w", err)
		}
	}

	return nil
}

// --- 3) Worker ---
// Worker menerima tugas (delivery), menjalankan simulasi pembuatan
// laporan dengan timeout, memperbarui status di Redis, dan mengirim
// hasilnya ke channel `results` untuk di-ack oleh handler.
func reportWorker(ctx context.Context, workerID int, tasks <-chan amqp.Delivery, results chan<- ReportResult, rdb *redis.Client) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("[Worker-%d] Context cancelled, exit", workerID)
			return
		case d, ok := <-tasks:
			if !ok {
				log.Printf("[Worker-%d] Tasks channel closed, exit", workerID)
				return
			}

			var req ReportRequest
			if err := json.Unmarshal(d.Body, &req); err != nil {
				log.Printf("[Worker-%d] Invalid message: %v", workerID, err)
				results <- ReportResult{
					RequestID:   "unknown",
					Status:      StatusFailed,
					GeneratedAt: time.Now(),
					Error:       "invalid message",
				}
				continue
			}

			// IN_PROGRESS
			if err := updateReportStatus(ctx, rdb, req.ID, StatusInProgress, "", ""); err != nil {
				log.Printf("[Worker-%d][%s] Failed to update status IN_PROGRESS: %v", workerID, req.ID, err)
			} else {
				log.Printf("[Worker-%d][%s] Set IN_PROGRESS", workerID, req.ID)
			}

			// Derive context with timeout per task
			taskCtx, cancel := context.WithTimeout(ctx, DefaultWorkerTimeout)
			data, err := simulateReportGeneration(taskCtx, req)
			cancel()

			if err != nil {
				_ = updateReportStatus(ctx, rdb, req.ID, StatusFailed, "", err.Error())
				log.Printf("[Worker-%d][%s] Processing failed: %v", workerID, req.ID, err)
				results <- ReportResult{
					RequestID:   req.ID,
					Status:      StatusFailed,
					GeneratedAt: time.Now(),
					Error:       err.Error(),
				}
				continue
			}

			_ = updateReportStatus(ctx, rdb, req.ID, StatusCompleted, data, "")
			log.Printf("[Worker-%d][%s] Completed", workerID, req.ID)
			results <- ReportResult{
				RequestID:   req.ID,
				Status:      StatusCompleted,
				GeneratedAt: time.Now(),
				ReportData:  data,
			}
		}
	}
}

// --- 4) Ack handler ---
// Karena kita menggunakan manual-ack di RabbitMQ, kita menyimpan
// delivery message di peta sementara (requestID -> delivery) supaya
// kita bisa melakukan Ack hanya setelah pekerjaan benar-benar selesai
// (COMPLETED atau FAILED). Ini mencegah kehilangan/duplikasi pesan.
type deliveryMap struct {
	mu   sync.RWMutex
	data map[string]amqp.Delivery
}

func (m *deliveryMap) Set(id string, d amqp.Delivery) {
	m.mu.Lock()
	m.data[id] = d
	m.mu.Unlock()
}

func (m *deliveryMap) Get(id string) (amqp.Delivery, bool) {
	m.mu.RLock()
	d, ok := m.data[id]
	m.mu.RUnlock()
	return d, ok
}

func (m *deliveryMap) Delete(id string) {
	m.mu.Lock()
	delete(m.data, id)
	m.mu.Unlock()
}

func resultAckHandler(ctx context.Context, results <-chan ReportResult, ch *amqp.Channel, rdb *redis.Client, dmap *deliveryMap) {
	// Handler terus membaca `results` dan melakukan Ack pada pesan
	// RabbitMQ ketika status hasil adalah final. Jika requestID tidak
	// ditemukan, log will note it and skip acking.
	for {
		select {
		case <-ctx.Done():
			log.Println("[AckHandler] Context cancelled, exit")
			return
		case res, ok := <-results:
			if !ok {
				log.Println("[AckHandler] Results channel closed, exit")
				return
			}

			if res.RequestID == "unknown" {
				// nothing to ack
				continue
			}

			d, ok := dmap.Get(res.RequestID)
			if !ok {
				log.Printf("[AckHandler] Missing delivery for %s", res.RequestID)
				continue
			}

			switch res.Status {
			case StatusCompleted, StatusFailed:
				if err := d.Ack(false); err != nil {
					log.Printf("[AckHandler][%s] Ack failed: %v", res.RequestID, err)
				} else {
					log.Printf("[AckHandler][%s] Acked (%s)", res.RequestID, res.Status)
				}
				dmap.Delete(res.RequestID)
			default:
				// not final
			}
		}
	}
}

// --- 5) Consumer orchestrator ---
// Fungsi ini mengorkestrasi consumer: membuka koneksi ke RabbitMQ,
// menerima pesan, menandai PENDING di Redis, menyimpan delivery
// untuk ack nanti, dan mendispatch pesan ke worker pool.
func startReportProcessor(ctx context.Context, wg *sync.WaitGroup, rdb *redis.Client, rabbitURL string) {
	defer wg.Done()

	conn, ch, err := connectRabbitMQ(ctx, rabbitURL)
	if err != nil {
		log.Printf("[Processor] RabbitMQ connect error: %v", err)
		return
	}
	defer conn.Close()
	defer ch.Close()

	msgs, err := ch.Consume(
		QueueName,
		"",    // consumer tag
		false, // autoAck=false (manual ack)
		false, // exclusive
		false, // no-local (deprecated)
		false, // no-wait
		nil,
	)
	if err != nil {
		log.Printf("[Processor] Consume error: %v", err)
		return
	}

	tasks := make(chan amqp.Delivery, 100)
	results := make(chan ReportResult, 100)

	// map requestID -> delivery
	dmap := &deliveryMap{
		data: make(map[string]amqp.Delivery),
	}

	// Launch workers
	var wwg sync.WaitGroup
	for i := 1; i <= DefaultNumWorkers; i++ {
		wwg.Add(1)
		go func(id int) {
			defer wwg.Done()
			reportWorker(ctx, id, tasks, results, rdb)
		}(i)
	}

	// Ack handler
	var ackWG sync.WaitGroup
	ackWG.Add(1)
	go func() {
		defer ackWG.Done()
		resultAckHandler(ctx, results, ch, rdb, dmap)
	}()

	// Main loop: receive deliveries, set Pending, dispatch to workers
loop:
	for {
		select {
		case <-ctx.Done():
			log.Println("[Processor] Context cancelled, stopping...")
			break loop
		case d, ok := <-msgs:
			if !ok {
				log.Println("[Processor] RabbitMQ channel closed")
				break loop
			}
			var req ReportRequest
			if err := json.Unmarshal(d.Body, &req); err != nil {
				log.Printf("[Processor] Invalid message: %v", err)
				// Ack to avoid redelivery loop
				_ = d.Ack(false)
				continue
			}

			// Trace: log bahwa kita menerima pesan dengan request ID
			log.Printf("[Processor] Received message: %s (type=%s)", req.ID, req.ReportType)

			// Set status pending
			// Simpan status PENDING sehingga klien tahu pesan sudah
			// diambil oleh sistem dan menunggu pemrosesan.
			if err := updateReportStatus(ctx, rdb, req.ID, StatusPending, "", ""); err != nil {
				log.Printf("[Processor] Failed to set PENDING for %s: %v", req.ID, err)
			} else {
				log.Printf("[Processor] Set PENDING for %s", req.ID)
			}

			dmap.Set(req.ID, d)
			select {
			case tasks <- d:
			case <-ctx.Done():
				break loop
			}
		}
	}

	// Cleanup
	close(tasks)
	wwg.Wait()
	close(results)
	ackWG.Wait()
	log.Println("[Processor] Stopped")
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// Env-configurable
	rabbitURL := getenv("RABBITMQ_URL", DefaultRabbitURL)
	redisURL := getenv("REDIS_URL", DefaultRedisURL)
	numReq := getEnvInt("NUM_PRODUCER_REQUESTS", DefaultNumProducerRequests)
	pubInterval := getEnvDuration("PUBLISH_INTERVAL", DefaultPublishInterval)

	// Buat context yang akan dibatalkan ketika menerima SIGINT/SIGTERM.
	// Ini memudahkan shutdown teratur (graceful shutdown).
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Connect
	rdb := connectRedis(ctx, redisURL)
	conn, ch, err := connectRabbitMQ(ctx, rabbitURL)
	if err != nil {
		log.Fatalf("Producer RabbitMQ connect error: %v", err)
	}
	defer conn.Close()
	defer ch.Close()

	var wg sync.WaitGroup

	// Start producer
	wg.Add(1)
	go produceReportRequests(ctx, &wg, ch, numReq, pubInterval)

	// Start consumer/processor
	wg.Add(1)
	go startReportProcessor(ctx, &wg, rdb, rabbitURL)

	<-ctx.Done()
	log.Println("Received shutdown signal, waiting for goroutines...")
	wg.Wait()
	log.Println("All done.")
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		_, err := fmt.Sscanf(v, "%d", &n)
		if err == nil {
			return n
		}
	}
	return def
}

func getEnvDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return def
}
