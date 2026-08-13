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

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
)

// ============================================
// Client
// ============================================

type Client struct {
	Conn     *websocket.Conn
	Username string
}

// ============================================
// Chat Message
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
// Online / Offline Status Message
// ============================================

type StatusMessage struct {
	Type     string `json:"type"`
	Username string `json:"username"`
	Status   string `json:"status"`
}

// ============================================
// Global Variables
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

	// Kafka Producer
	kafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "chat-messages",
		Balancer: &kafka.LeastBytes{},
	}

	// Kafka Consumer
	kafkaReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "chat-messages",
		GroupID: "connectify-chat-consumer",
	})
)

func connectDatabase() {

	var err error

	// Change the password if your MySQL root password is different
	dsn := "root:navee@123@tcp(localhost:3306)/connectify"

	db, err = sql.Open("mysql", dsn)

	if err != nil {
		log.Fatal("MySQL connection error:", err)
	}

	err = db.Ping()

	if err != nil {
		log.Fatal("MySQL ping error:", err)
	}

	log.Println("MySQL Connected successfully")
}

// ============================================
// MAIN
// ============================================

func main() {

	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/ws", handleWebSocket)
	http.HandleFunc("/history", historyHandler)

	// Serve frontend
	http.Handle("/", http.FileServer(http.Dir("../frontend")))

	port := os.Getenv("PORT")

	if port == "" {
		port = "8080"
	}

	log.Println("================================")
	log.Println("Connectify Chat Server")
	log.Println("Port:", port)
	log.Println("Kafka: localhost:9092")
	log.Println("Topic: chat-messages")
	log.Println("================================")

	connectDatabase()

	// Start Kafka Consumer
	go startKafkaConsumer()

	// Start HTTP + WebSocket Server
	err := http.ListenAndServe(":"+port, nil)

	if err != nil {
		log.Fatal(err)
	}
}

// ============================================
// HEALTH CHECK
// ============================================

func healthHandler(w http.ResponseWriter, r *http.Request) {

	w.WriteHeader(http.StatusOK)

	w.Write([]byte("Chat server is running"))
}

func historyHandler(w http.ResponseWriter, r *http.Request) {

	sender := r.URL.Query().Get("sender")
	receiver := r.URL.Query().Get("receiver")

	if sender == "" || receiver == "" {
		http.Error(w, "sender and receiver are required", http.StatusBadRequest)
		return
	}

	rows, err := db.Query(`
		SELECT id, sender, receiver, message, timestamp, status
		FROM chat_messages
		WHERE
			(sender = ? AND receiver = ?)
			OR
			(sender = ? AND receiver = ?)
		ORDER BY id ASC
	`,
		sender,
		receiver,
		receiver,
		sender,
	)

	if err != nil {
		log.Println("History query error:", err)
		http.Error(w, "Failed to load history", http.StatusInternalServerError)
		return
	}

	defer rows.Close()

	var history []Message

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
			log.Println("History scan error:", err)
			continue
		}

		history = append(history, message)
	}

	w.Header().Set("Content-Type", "application/json")

	json.NewEncoder(w).Encode(history)
}

// ============================================
// WEBSOCKET HANDLER
// ============================================

func handleWebSocket(w http.ResponseWriter, r *http.Request) {

	username := r.URL.Query().Get("username")
	receiver := r.URL.Query().Get("receiver")

	if username == "" {
		username = "User"
	}

	if receiver == "" {
		receiver = "User"
	}

	// Upgrade HTTP connection to WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)

	if err != nil {
		log.Println("WebSocket upgrade error:", err)
		return
	}

	// Create client
	client := &Client{
		Conn:     conn,
		Username: username,
	}

	// Add client
	clientsMu.Lock()
	clients[client] = true
	clientsMu.Unlock()

	log.Printf(
		"%s connected and chatting with %s",
		username,
		receiver,
	)

	// ========================================
	// STEP 8
	// USER ONLINE
	// ========================================

	broadcastStatus(username, "online")

	// ========================================
	// USER DISCONNECT
	// ========================================

	defer func() {

		clientsMu.Lock()
		delete(clients, client)
		clientsMu.Unlock()

		// Notify other users
		broadcastStatus(username, "offline")

		conn.Close()

		log.Println(
			"User disconnected:",
			username,
		)

	}()

	// ========================================
	// READ MESSAGES FROM WEBSOCKET
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

		// Set sender and receiver from connection
		message.Sender = username
		message.Receiver = receiver
		message.Timestamp = time.Now().Format("03:04 PM")
		message.Status = "sent"

		log.Printf(
			"Message: %s → %s : %s",
			message.Sender,
			message.Receiver,
			message.Message,
		)

		// Send message to Kafka
		sendToKafka(message)
	}
}

// ============================================
// KAFKA PRODUCER
// ============================================

func sendToKafka(message Message) {

	data, err := json.Marshal(message)

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
// STEP 7
// KAFKA CONSUMER
// Kafka → WebSocket
// ============================================

func startKafkaConsumer() {

	log.Println("Kafka Consumer started")

	for {

		message, err := kafkaReader.ReadMessage(
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

		messageID := saveMessageToDatabase(chatMessage)

		if messageID == 0 {
			continue
		}

		chatMessage.ID = messageID

		// Send message to sender and receiver
		sendToReceiver(chatMessage)
	}
}

// ============================================
// SAVE CHAT MESSAGE TO MYSQL
// ============================================

func saveMessageToDatabase(message Message) int {

	result, err := db.Exec(
		`INSERT INTO chat_messages
		(sender, receiver, message, timestamp, status)
		VALUES (?, ?, ?, ?, ?)`,
		message.Sender,
		message.Receiver,
		message.Message,
		message.Timestamp,
		message.Status,
	)

	if err != nil {

		log.Println(
			"MySQL save error:",
			err,
		)

		return 0
	}

	id, err := result.LastInsertId()

	if err != nil {

		log.Println(
			"LastInsertId error:",
			err,
		)

		return 0
	}

	log.Printf(
		"Message saved to MySQL: ID=%d %s → %s",
		id,
		message.Sender,
		message.Receiver,
	)

	return int(id)
}

// ============================================
// SEND MESSAGE TO WEBSOCKET CLIENTS
// ============================================

func sendToReceiver(message Message) {

	data, err := json.Marshal(message)

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

		// Send to sender and receiver
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
				delete(clients, client)

				continue
			}

			log.Printf(
				"Message delivered to %s",
				client.Username,
			)

			// ====================================
			// RECEIVER GOT THE MESSAGE
			// ====================================

			if client.Username == message.Receiver {

				go markMessageDelivered(
					message.ID,
				)
			}
		}
	}
}
func markMessageDelivered(messageID int) {

	_, err := db.Exec(
		`UPDATE chat_messages
		SET status = 'delivered'
		WHERE id = ?`,
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

	// Send delivery update to sender
	sendDeliveryUpdate(messageID)
}

func sendDeliveryUpdate(messageID int) {

	type DeliveryUpdate struct {
		Type      string `json:"type"`
		MessageID int    `json:"messageId"`
		Status    string `json:"status"`
	}

	update := DeliveryUpdate{
		Type:      "delivery",
		MessageID: messageID,
		Status:    "delivered",
	}

	data, err := json.Marshal(update)

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
// STEP 8
// ONLINE / OFFLINE STATUS
// ============================================

func broadcastStatus(username string, status string) {

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

			delete(clients, client)

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
