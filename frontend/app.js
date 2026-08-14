// ========================================
// GET USERNAME AND RECEIVER
// ========================================

const params = new URLSearchParams(window.location.search);

let username = params.get("username");
let receiver = params.get("receiver");

// If username is not provided in URL,
// ask the user for it.

if (!username) {
    username = prompt("Enter your username:");

    if (!username || username.trim() === "") {
        username = "User";
    }
}

// If receiver is not provided in URL,
// ask the user for it.

if (!receiver) {
    receiver = prompt("Enter receiver username:");

    if (!receiver || receiver.trim() === "") {
        receiver = "User";
    }
}

username = username.trim();
receiver = receiver.trim();


// ========================================
// DISPLAY USER INFORMATION
// ========================================

console.log("================================");
console.log("Connectify Chat");
console.log("Username :", username);
console.log("Receiver :", receiver);
console.log("================================");


// ========================================
// WEBSOCKET URL
// ========================================

const protocol =
    window.location.protocol === "https:"
        ? "wss://"
        : "ws://";

const socketUrl =
    protocol +
    window.location.host +
    "/ws?username=" +
    encodeURIComponent(username) +
    "&receiver=" +
    encodeURIComponent(receiver);

console.log("WebSocket URL:", socketUrl);


// ========================================
// CREATE WEBSOCKET CONNECTION
// ========================================

const socket = new WebSocket(socketUrl);


// ========================================
// HTML ELEMENTS
// ========================================

const messageInput =
    document.getElementById("messageInput");

const sendButton =
    document.getElementById("sendButton");

const messages =
    document.getElementById("messages");

const status =
    document.getElementById("status");


// ========================================
// CHECK HTML ELEMENTS
// ========================================

if (!messageInput) {
    console.error("messageInput element not found");
}

if (!sendButton) {
    console.error("sendButton element not found");
}

if (!messages) {
    console.error("messages element not found");
}

if (!status) {
    console.error("status element not found");
}


// ========================================
// LOAD CHAT HISTORY FROM POSTGRESQL
// ========================================

async function loadChatHistory() {

    try {

        console.log(
            "Loading chat history:",
            username,
            "↔",
            receiver
        );

        const response = await fetch(
            "/history?sender=" +
            encodeURIComponent(username) +
            "&receiver=" +
            encodeURIComponent(receiver)
        );

        if (!response.ok) {

            throw new Error(
                "Failed to load chat history"
            );
        }

        const history =
            await response.json();

        console.log(
            "Chat history:",
            history
        );

        // Clear existing messages before loading history

        messages.innerHTML = "";

        history.forEach(function (data) {

            if (data.sender === username) {

                addMessage(
                    data.message,
                    "sent",
                    data.timestamp,
                    data.status,
                    data.id
                );

            } else {

                addMessage(
                    data.message,
                    "received",
                    data.timestamp,
                    data.status,
                    data.id
                );
            }
        });

    } catch (error) {

        console.error(
            "History loading error:",
            error
        );
    }
}


// ========================================
// WEBSOCKET CONNECTED
// ========================================

socket.onopen = function () {

    console.log(
        "================================"
    );

    console.log(
        "WebSocket connected successfully"
    );

    console.log(
        "Username:",
        username
    );

    console.log(
        "Receiver:",
        receiver
    );

    console.log(
        "================================"
    );

    status.textContent =
        receiver + " Online";
};


// ========================================
// WEBSOCKET DISCONNECTED
// ========================================

socket.onclose = function () {

    status.textContent =
        receiver + " Offline";

    console.log(
        "WebSocket disconnected:",
        username
    );
};


// ========================================
// WEBSOCKET ERROR
// ========================================

socket.onerror = function (error) {

    console.error(
        "WebSocket error:",
        error
    );

    status.textContent =
        "Connection error";
};


// ========================================
// RECEIVE WEBSOCKET MESSAGE
// ========================================

socket.onmessage = function (event) {

    try {

        const data =
            JSON.parse(event.data);

        console.log(
            "Received from server:",
            data
        );


        // ====================================
        // ONLINE / OFFLINE STATUS
        // ====================================

        if (data.type === "status") {

            console.log(
                "User status:",
                data.username,
                data.status
            );

            if (
                data.username === receiver
            ) {

                if (
                    data.status === "online"
                ) {

                    status.textContent =
                        receiver + " Online";

                } else if (
                    data.status === "offline"
                ) {

                    status.textContent =
                        receiver + " Offline";
                }
            }

            return;
        }


        // ====================================
        // DELIVERY STATUS
        // ====================================

        if (data.type === "delivery") {

            console.log(
                "Message delivered:",
                data.messageId
            );

            updateMessageStatus(
                data.messageId,
                data.status
            );

            return;
        }


        // ====================================
        // CHAT MESSAGE
        // ====================================

        if (data.message) {

            console.log(
                "Chat message:",
                data.sender,
                "→",
                data.receiver,
                ":",
                data.message
            );

            if (
                data.sender === username
            ) {

                addMessage(
                    data.message,
                    "sent",
                    data.timestamp,
                    data.status,
                    data.id
                );

            } else if (
                data.sender === receiver &&
                data.receiver === username
            ) {

                addMessage(
                    data.message,
                    "received",
                    data.timestamp,
                    data.status,
                    data.id
                );
            }
        }

    } catch (error) {

        console.error(
            "WebSocket message parsing error:",
            error
        );
    }
};


// ========================================
// SEND MESSAGE
// ========================================

function sendMessage() {

    const message =
        messageInput.value.trim();

    // Don't send empty message

    if (message === "") {

        return;
    }


    // ====================================
    // CHECK WEBSOCKET CONNECTION
    // ====================================

    if (
        socket.readyState !==
        WebSocket.OPEN
    ) {

        alert(
            "WebSocket is not connected"
        );

        return;
    }


    // ====================================
    // MESSAGE DATA
    // ====================================

    const data = {

        message: message

    };


    console.log(
        "Sending:",
        username,
        "→",
        receiver,
        ":",
        message
    );


    // ====================================
    // SEND TO GO SERVER
    // ====================================

    socket.send(
        JSON.stringify(data)
    );


    // ====================================
    // CLEAR INPUT
    // ====================================

    messageInput.value = "";

    messageInput.focus();
}


// ========================================
// ADD MESSAGE TO CHAT
// ========================================

function addMessage(
    message,
    type,
    timestamp,
    messageStatus,
    messageId
) {

    const messageElement =
        document.createElement("div");


    // ====================================
    // MESSAGE CLASS
    // ====================================

    messageElement.classList.add(
        "message",
        type
    );


    // ====================================
    // STORE MESSAGE ID
    // ====================================

    if (messageId !== undefined &&
        messageId !== null) {

        messageElement.dataset.messageId =
            messageId;
    }


    // ====================================
    // MESSAGE TEXT
    // ====================================

    const textElement =
        document.createElement("span");

    textElement.classList.add(
        "message-text"
    );

    textElement.textContent =
        message;


    // ====================================
    // MESSAGE TIME
    // ====================================

    const timeElement =
        document.createElement("small");

    timeElement.classList.add(
        "message-time"
    );

    timeElement.textContent =
        timestamp || "";


    // ====================================
    // MESSAGE STATUS
    // ====================================

    const statusElement =
        document.createElement("span");

    statusElement.classList.add(
        "message-status"
    );


    if (type === "sent") {

        if (
            messageStatus ===
            "delivered"
        ) {

            statusElement.textContent =
                "✓✓";

        } else {

            statusElement.textContent =
                "✓";
        }
    }


    // ====================================
    // ADD MESSAGE TEXT
    // ====================================

    messageElement.appendChild(
        textElement
    );


    // ====================================
    // ADD TIME
    // ====================================

    messageElement.appendChild(
        timeElement
    );


    // ====================================
    // ADD STATUS
    // ====================================

    if (type === "sent") {

        messageElement.appendChild(
            statusElement
        );
    }


    // ====================================
    // ADD MESSAGE TO CHAT
    // ====================================

    messages.appendChild(
        messageElement
    );


    // ====================================
    // SCROLL TO LATEST MESSAGE
    // ====================================

    messages.scrollTop =
        messages.scrollHeight;
}


// ========================================
// UPDATE MESSAGE DELIVERY STATUS
// ========================================

function updateMessageStatus(
    messageId,
    messageStatus
) {

    console.log(
        "Updating message:",
        messageId,
        "Status:",
        messageStatus
    );


    const messageElement =
        document.querySelector(
            `[data-message-id="${messageId}"]`
        );


    if (!messageElement) {

        console.log(
            "Message element not found:",
            messageId
        );

        return;
    }


    const statusElement =
        messageElement.querySelector(
            ".message-status"
        );


    if (!statusElement) {

        console.log(
            "Message status element not found"
        );

        return;
    }


    if (
        messageStatus ===
        "delivered"
    ) {

        statusElement.textContent =
            "✓✓";
    }
}


// ========================================
// SEND BUTTON CLICK
// ========================================

if (sendButton) {

    sendButton.addEventListener(
        "click",
        sendMessage
    );
}


// ========================================
// ENTER KEY TO SEND
// ========================================

if (messageInput) {

    messageInput.addEventListener(
        "keydown",
        function (event) {

            if (event.key === "Enter") {

                event.preventDefault();

                sendMessage();
            }
        }
    );
}


// ========================================
// LOAD HISTORY WHEN PAGE OPENS
// ========================================

loadChatHistory();