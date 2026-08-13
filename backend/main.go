package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

// ============================================
// CLIENT
// ============================================

type Client struct {
	Conn     *websocket.Conn
	Username string
}

// ============================================
// CHAT MESSAGE
// ============================================

type Message struct {
	ID        int    `json:"id"`
	Sender    string `json:"sender"`
	Receiver  string `json:"receiver"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
	Status    string `json:"status"`
}

// ============================================
// STATUS MESSAGE
// ============================================

type StatusMessage struct {
	Type     string `json:"type"`
	Username string `json:"username"`
	Status   string `json:"status"`
}

// ============================================
// DELIVERY UPDATE
// ============================================

type DeliveryUpdate struct {
	Type      string `json:"type"`
	MessageID int    `json:"messageId"`
	Status    string `json:"status"`
}

// ============================================
// GLOBAL VARIABLES
// ============================================

var (
	clients   = make(map[*Client]bool)
	clientsMu sync.Mutex

	db *sql.DB

	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	kafkaWriter *kafka.Writer
	kafkaReader *kafka.Reader
)

// ============================================
// ENVIRONMENT VARIABLE
// ============================================

func getEnv(key string, defaultValue string) string {

	value := os.Getenv(key)

	if value == "" {
		return defaultValue
	}

	return value
}

// ============================================
// DATABASE CONNECTION
// ============================================

func connectDatabase() {

	var err error

	// Render DATABASE_URL
	dsn := os.Getenv("DATABASE_URL")

	// Local PostgreSQL
	if dsn == "" {

		dsn = "postgres://postgres:Navee@123@localhost:5432/connectify?sslmode=disable"

		log.Println(
			"DATABASE_URL not found. Using local PostgreSQL.",
		)
	}

	db, err = sql.Open(
		"postgres",
		dsn,
	)

	if err != nil {
		log.Fatal(
			"PostgreSQL connection error:",
			err,
		)
	}

	// Test connection
	err = db.Ping()

	if err != nil {
		log.Fatal(
			"PostgreSQL ping error:",
			err,
		)
	}

	// Show current database
	var currentDB string

	err = db.QueryRow(
		"SELECT current_database()",
	).Scan(&currentDB)

	if err != nil {
		log.Fatal(
			"Could not get current database:",
			err,
		)
	}

	log.Println(
		"Connected PostgreSQL database:",
		currentDB,
	)

	// Show current schema
	var currentSchema string

	err = db.QueryRow(
		"SELECT current_schema()",
	).Scan(&currentSchema)

	if err != nil {
		log.Println(
			"Could not get current schema:",
			err,
		)
	} else {

		log.Println(
			"Connected PostgreSQL schema:",
			currentSchema,
		)
	}

	log.Println(
		"PostgreSQL Connected successfully",
	)

	// ========================================
	// CREATE TABLE AUTOMATICALLY
	// ========================================

	createChatTable()
}

// ============================================
// CREATE CHAT TABLE
// ============================================

func createChatTable() {

	query := `
	CREATE TABLE IF NOT EXISTS public.chat_messages (

		id SERIAL PRIMARY KEY,

		sender VARCHAR(100) NOT NULL,

		receiver VARCHAR(100) NOT NULL,

		message TEXT NOT NULL,

		timestamp TIMESTAMP NOT NULL,

		status VARCHAR(20) DEFAULT 'sent'
	);
	`

	_, err := db.Exec(query)

	if err != nil {

		log.Fatal(
			"Could not create chat_messages table:",
			err,
		)
	}

	log.Println(
		"PostgreSQL table chat_messages is ready",
	)
}

// ============================================
// KAFKA CONNECTION
// ============================================

func connectKafka() {

	kafkaBroker := getEnv(
		"KAFKA_BROKER",
		"localhost:9092",
	)

	kafkaTopic := getEnv(
		"KAFKA_TOPIC",
		"chat-messages",
	)

	kafkaGroup := getEnv(
		"KAFKA_GROUP",
		"connectify-chat-consumer",
	)

	log.Println(
		"Kafka Broker:",
		kafkaBroker,
	)

	log.Println(
		"Kafka Topic:",
		kafkaTopic,
	)

	log.Println(
		"Kafka Group:",
		kafkaGroup,
	)

	// ========================================
	// KAFKA PRODUCER
	// ========================================

	kafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    kafkaTopic,
		Balancer: &kafka.LeastBytes{},
	}

	// ========================================
	// KAFKA CONSUMER
	// ========================================

	kafkaReader = kafka.NewReader(
		kafka.ReaderConfig{
			Brokers: []string{
				kafkaBroker,
			},
			Topic:   kafkaTopic,
			GroupID: kafkaGroup,
		},
	)

	log.Println(
		"Kafka connection configured",
	)
}

// ============================================
// MAIN
// ============================================

func main() {

	// ========================================
	// ROUTES
	// ========================================

	http.HandleFunc(
		"/health",
		healthHandler,
	)

	http.HandleFunc(
		"/ws",
		handleWebSocket,
	)

	http.HandleFunc(
		"/history",
		historyHandler,
	)

	// ========================================
	// SERVE FRONTEND
	// ========================================

	http.Handle(
		"/",
		http.FileServer(
			http.Dir("../frontend"),
		),
	)

	// ========================================
	// RENDER PORT
	// ========================================

	port := getEnv(
		"PORT",
		"8080",
	)

	log.Println(
		"================================",
	)

	log.Println(
		"Connectify Chat Server",
	)

	log.Println(
		"Port:",
		port,
	)

	log.Println(
		"================================",
	)

	// ========================================
	// DATABASE
	// ========================================

	connectDatabase()

	// ========================================
	// KAFKA
	// ========================================

	connectKafka()

	// ========================================
	// START KAFKA CONSUMER
	// ========================================

	go startKafkaConsumer()

	// ========================================
	// START SERVER
	// ========================================

	log.Println(
		"Server starting on port:",
		port,
	)

	err := http.ListenAndServe(
		"0.0.0.0:"+port,
		nil,
	)

	if err != nil {

		log.Fatal(err)
	}
}

// ============================================
// HEALTH CHECK
// ============================================

func healthHandler(
	w http.ResponseWriter,
	r *http.Request,
) {

	w.WriteHeader(
		http.StatusOK,
	)

	w.Write(
		[]byte(
			"Connectify Chat Server is running",
		),
	)
}

// ============================================
// CHAT HISTORY
// ============================================

func historyHandler(
	w http.ResponseWriter,
	r *http.Request,
) {

	sender := r.URL.Query().Get(
		"sender",
	)

	receiver := r.URL.Query().Get(
		"receiver",
	)

	if sender == "" || receiver == "" {

		http.Error(
			w,
			"sender and receiver are required",
			http.StatusBadRequest,
		)

		return
	}

	log.Printf(
		"Loading history: %s ↔ %s",
		sender,
		receiver,
	)

	rows, err := db.Query(
		`
		SELECT
			id,
			sender,
			receiver,
			message,
			timestamp,
			status
		FROM public.chat_messages
		WHERE
			(sender = $1 AND receiver = $2)
			OR
			(sender = $3 AND receiver = $4)
		ORDER BY id ASC
		`,
		sender,
		receiver,
		receiver,
		sender,
	)

	if err != nil {

		log.Println(
			"History query error:",
			err,
		)

		http.Error(
			w,
			"Failed to load history",
			http.StatusInternalServerError,
		)

		return
	}

	defer rows.Close()

	history := []Message{}

	for rows.Next() {

		var message Message

		err := rows.Scan(
			&message.ID,
			&message.Sender,
			&message.Receiver,
			&message.Message,
			&message.Timestamp,
			&message.Status,
		)

		if err != nil {

			log.Println(
				"History scan error:",
				err,
			)

			continue
		}

		history = append(
			history,
			message,
		)
	}

	if err := rows.Err(); err != nil {

		log.Println(
			"History rows error:",
			err,
		)
	}

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	json.NewEncoder(w).Encode(
		history,
	)
}

// ============================================
// WEBSOCKET HANDLER
// ============================================

func handleWebSocket(
	w http.ResponseWriter,
	r *http.Request,
) {

	username := r.URL.Query().Get(
		"username",
	)

	receiver := r.URL.Query().Get(
		"receiver",
	)

	if username == "" {

		username = "User"
	}

	if receiver == "" {

		receiver = "User"
	}

	// ========================================
	// UPGRADE TO WEBSOCKET
	// ========================================

	conn, err := upgrader.Upgrade(
		w,
		r,
		nil,
	)

	if err != nil {

		log.Println(
			"WebSocket upgrade error:",
			err,
		)

		return
	}

	// ========================================
	// CREATE CLIENT
	// ========================================

	client := &Client{
		Conn:     conn,
		Username: username,
	}

	clientsMu.Lock()

	clients[client] = true

	clientsMu.Unlock()

	log.Printf(
		"%s connected and chatting with %s",
		username,
		receiver,
	)

	// ========================================
	// USER ONLINE
	// ========================================

	broadcastStatus(
		username,
		"online",
	)

	// ========================================
	// DISCONNECT
	// ========================================

	defer func() {

		clientsMu.Lock()

		delete(
			clients,
			client,
		)

		clientsMu.Unlock()

		broadcastStatus(
			username,
			"offline",
		)

		conn.Close()

		log.Println(
			"User disconnected:",
			username,
		)
	}()

	// ========================================
	// READ MESSAGES
	// ========================================

	for {

		_, data, err := conn.ReadMessage()

		if err != nil {

			log.Println(
				"Read error:",
				err,
			)

			break
		}

		var message Message

		err = json.Unmarshal(
			data,
			&message,
		)

		if err != nil {

			log.Println(
				"JSON error:",
				err,
			)

			continue
		}

		// ====================================
		// SET MESSAGE INFORMATION
		// ====================================

		message.Sender = username

		message.Receiver = receiver

		message.Timestamp =
			time.Now().Format(
				"2006-01-02 15:04:05",
			)

		message.Status = "sent"

		log.Printf(
			"Message: %s → %s : %s",
			message.Sender,
			message.Receiver,
			message.Message,
		)

		// ====================================
		// SEND TO KAFKA
		// ====================================

		sendToKafka(
			message,
		)
	}
}

// ============================================
// KAFKA PRODUCER
// ============================================

func sendToKafka(
	message Message,
) {

	data, err := json.Marshal(
		message,
	)

	if err != nil {

		log.Println(
			"JSON marshal error:",
			err,
		)

		return
	}

	err = kafkaWriter.WriteMessages(
		context.Background(),
		kafka.Message{
			Key:   []byte(message.Receiver),
			Value: data,
		},
	)

	if err != nil {

		log.Println(
			"Kafka write error:",
			err,
		)

		return
	}

	log.Printf(
		"Kafka message sent: %s → %s",
		message.Sender,
		message.Receiver,
	)
}

// ============================================
// KAFKA CONSUMER
// Kafka → PostgreSQL → WebSocket
// ============================================

func startKafkaConsumer() {

	log.Println(
		"Kafka Consumer started",
	)

	for {

		message, err :=
			kafkaReader.ReadMessage(
				context.Background(),
			)

		if err != nil {

			log.Println(
				"Kafka read error:",
				err,
			)

			continue
		}

		log.Println(
			"Message received from Kafka:",
			string(message.Value),
		)

		var chatMessage Message

		err = json.Unmarshal(
			message.Value,
			&chatMessage,
		)

		if err != nil {

			log.Println(
				"Kafka JSON parsing error:",
				err,
			)

			continue
		}

		log.Printf(
			"Kafka Chat: %s → %s : %s",
			chatMessage.Sender,
			chatMessage.Receiver,
			chatMessage.Message,
		)

		// ====================================
		// SAVE TO POSTGRESQL
		// ====================================

		messageID :=
			saveMessageToDatabase(
				chatMessage,
			)

		if messageID == 0 {

			continue
		}

		chatMessage.ID = messageID

		// ====================================
		// SEND TO CLIENTS
		// ====================================

		sendToReceiver(
			chatMessage,
		)
	}
}

// ============================================
// SAVE MESSAGE TO POSTGRESQL
// ============================================

func saveMessageToDatabase(
	message Message,
) int {

	var messageID int

	err := db.QueryRow(
		`
		INSERT INTO public.chat_messages
		(
			sender,
			receiver,
			message,
			timestamp,
			status
		)
		VALUES
		(
			$1,
			$2,
			$3,
			$4,
			$5
		)
		RETURNING id
		`,
		message.Sender,
		message.Receiver,
		message.Message,
		message.Timestamp,
		message.Status,
	).Scan(
		&messageID,
	)

	if err != nil {

		log.Println(
			"PostgreSQL save error:",
			err,
		)

		return 0
	}

	log.Printf(
		"Message saved to PostgreSQL: ID=%d %s → %s",
		messageID,
		message.Sender,
		message.Receiver,
	)

	return messageID
}

// ============================================
// SEND MESSAGE TO CLIENTS
// ============================================

func sendToReceiver(
	message Message,
) {

	data, err := json.Marshal(
		message,
	)

	if err != nil {

		log.Println(
			"WebSocket JSON error:",
			err,
		)

		return
	}

	clientsMu.Lock()

	defer clientsMu.Unlock()

	for client := range clients {

		if client.Username == message.Receiver ||
			client.Username == message.Sender {

			err := client.Conn.WriteMessage(
				websocket.TextMessage,
				data,
			)

			if err != nil {

				log.Println(
					"WebSocket send error:",
					err,
				)

				client.Conn.Close()

				delete(
					clients,
					client,
				)

				continue
			}

			log.Printf(
				"Message delivered to %s",
				client.Username,
			)

			// Receiver received message
			if client.Username ==
				message.Receiver {

				go markMessageDelivered(
					message.ID,
				)
			}
		}
	}
}

// ============================================
// MARK MESSAGE DELIVERED
// ============================================

func markMessageDelivered(
	messageID int,
) {

	_, err := db.Exec(
		`
		UPDATE public.chat_messages
		SET status = 'delivered'
		WHERE id = $1
		`,
		messageID,
	)

	if err != nil {

		log.Println(
			"Delivery status update error:",
			err,
		)

		return
	}

	log.Printf(
		"Message ID %d marked as delivered",
		messageID,
	)

	sendDeliveryUpdate(
		messageID,
	)
}

// ============================================
// SEND DELIVERY UPDATE
// ============================================

func sendDeliveryUpdate(
	messageID int,
) {

	update := DeliveryUpdate{
		Type:      "delivery",
		MessageID: messageID,
		Status:    "delivered",
	}

	data, err := json.Marshal(
		update,
	)

	if err != nil {

		log.Println(
			"Delivery JSON error:",
			err,
		)

		return
	}

	clientsMu.Lock()

	defer clientsMu.Unlock()

	for client := range clients {

		err := client.Conn.WriteMessage(
			websocket.TextMessage,
			data,
		)

		if err != nil {

			log.Println(
				"Delivery update error:",
				err,
			)
		}
	}
}

// ============================================
// ONLINE / OFFLINE STATUS
// ============================================

func broadcastStatus(
	username string,
	status string,
) {

	data, err := json.Marshal(
		StatusMessage{
			Type:     "status",
			Username: username,
			Status:   status,
		},
	)

	if err != nil {

		log.Println(
			"Status JSON error:",
			err,
		)

		return
	}

	clientsMu.Lock()

	defer clientsMu.Unlock()

	for client := range clients {

		err := client.Conn.WriteMessage(
			websocket.TextMessage,
			data,
		)

		if err != nil {

			log.Println(
				"Status WebSocket error:",
				err,
			)

			client.Conn.Close()

			delete(
				clients,
				client,
			)

			continue
		}

		log.Printf(
			"Status sent to %s: %s is %s",
			client.Username,
			username,
			status,
		)
	}
}
