1. Framework Go

Saya sudah terbiasa dengan PHP Laravel, Node.js/Express.js, dan Golang di proyek nyata, serta mulai eksplor Golang dengan pelatihan dan sertifikasi Backend Engineer Golang. Untuk Go, framework yang saya kenal adalah Gin (REST API cepat dan ringan) serta gRPC (komunikasi antar microservice dengan Protobuf). Pengalaman DI (dependency injection) saya lebih banyak di Spring Boot/Node.js, dan dalam konteks Go bisa digantikan dengan Wire/Fx.

2. Concurrency di Go

Saya memahami bahwa Go unggul di concurrency dengan goroutine (thread ringan) dan channel (komunikasi aman antar goroutine). Konsep ini mirip dengan asynchronous programming di Node.js yang sering saya gunakan. Dengan Go, worker pool dan bounded concurrency bisa diatur lebih efisien menggunakan goroutine.

3. WaitGroup + Channel

WaitGroup berfungsi menunggu semua goroutine selesai, channel dipakai untuk mengirim data antar goroutine. Kombinasinya berguna untuk job paralel yang hasilnya tetap dikumpulkan. Ini konsep yang mirip dengan menunggu beberapa promise di JavaScript (Promise.all) tapi dengan kontrol manual.

4. Goroutine

Goroutine adalah unit eksekusi ringan di Go, semacam thread kecil yang dikelola runtime. Kalau di Node.js saya biasa menggunakan async/await untuk concurrency, di Go hal yang sama dilakukan dengan goroutine. Praktis untuk parallel request API atau worker background.

5. Queueing di Go

Saya biasa mengerjakan antrian dengan Redis dan RabbitMQ. Di Go, queue sederhana bisa pakai channel (in-memory), sedangkan untuk durability dan distribusi lebih baik pakai RabbitMQ/Kafka dengan library resmi. Prinsipnya sama seperti implementasi job queue di Node.js dengan Redis.

6. Komunikasi antar microservice

Untuk sinkron, Go mendukung REST API (Gin/Echo) dan gRPC. Untuk async, saya sudah biasa pakai Redis/RabbitMQ di Node.js dan konsepnya sama bisa diimplementasikan di Go. Komunikasi terbaik tergantung kebutuhan: low latency → gRPC, event-driven → broker.

7. Contoh komunikasi antar service

Misalnya, Service A memanggil Service B dengan REST API memakai timeout agar tidak hang. Untuk komunikasi async, Service A publish event ke RabbitMQ, lalu Service B konsumsi event itu. Pola ini mirip yang sudah saya kerjakan dengan Laravel + Redis pub/sub.

8. Penanganan error di Go

Go tidak punya exception, semua error ditangani eksplisit(tidak terbelit-belit). Error bisa dibungkus (%w) dan diperiksa (errors.Is/As). Ini mirip dengan pengecekan return error di PHP/Node.js tapi lebih ketat. Saya terbiasa debugging error stack di Laravel/Node.js, dan di Go konsepnya lebih eksplisit.

9. Error logging

Pengalaman saya logging dengan Laravel (Monolog) dan Node.js (Winston). Di Go bisa pakai log/slog atau zerolog untuk structured logging (JSON). Untuk sistem besar, log sebaiknya dikirim ke ELK atau Grafana Loki supaya bisa difilter berdasarkan trace_id atau user_id.

10. Ongkos kirim

Perhitungannya: cek berat aktual vs volumetrik, ambil yang lebih besar, kalikan jumlah, lalu bulatkan ke atas per kg. Dari contoh: Produk A = 1 kg (Rp10.000), Produk B = 41 kg (Rp410.000). Total ongkos Rp420.000.

11. Kesulitan di Go

Kesulitan saya saat belajar Go adalah adaptasi dengan cara penanganan error yang eksplisit (berbeda dengan exception di PHP/Java/Node.js), serta concurrency yang butuh pemahaman context agar tidak ada goroutine leak. Saya atasi dengan latihan coding, sertifikasi HackerRank Go, dan praktik membuat project kecil.