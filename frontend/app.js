const params = new URLSearchParams(window.location.search);

const username = params.get("username") || "User";
const receiver = params.get("receiver") || "User";

const socket = new WebSocket(
    "ws://" +
    window.location.host +
    "/ws?username=" +
    encodeURIComponent(username) +
    "&receiver=" +
    encodeURIComponent(receiver)
);

const messageInput = document.getElementById("messageInput");
const sendButton = document.getElementById("sendButton");
const messages = document.getElementById("messages");
const status = document.getElementById("status");


// ========================================
// LOAD CHAT HISTORY FROM MYSQL
// ========================================

async function loadChatHistory() {

    try {

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

        const history = await response.json();

        console.log(
            "Chat history:",
            history
        );

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
        "Connected as:",
        username
    );

    status.textContent = "Online";
};


// ========================================
// WEBSOCKET DISCONNECTED
// ========================================

socket.onclose = function () {

    status.textContent = "Offline";

    console.log(
        "Disconnected:",
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

    status.textContent = "Connection error";
};


// ========================================
// RECEIVE WEBSOCKET MESSAGE
// ========================================

socket.onmessage = function (event) {

    const data = JSON.parse(event.data);

    console.log(
        "Received:",
        data
    );


    // ====================================
    // ONLINE / OFFLINE STATUS
    // ====================================

    if (data.type === "status") {

        console.log(
            data.username,
            "is",
            data.status
        );

        if (data.username === receiver) {

            if (data.status === "online") {

                status.textContent = "Online";

            } else if (data.status === "offline") {

                status.textContent = "Offline";
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
    }
};


// ========================================
// SEND MESSAGE
// ========================================

function sendMessage() {

    const message =
        messageInput.value.trim();

    if (message === "") {
        return;
    }


    if (
        socket.readyState !==
        WebSocket.OPEN
    ) {

        alert(
            "WebSocket is not connected"
        );

        return;
    }


    const data = {
        message: message
    };


    socket.send(
        JSON.stringify(data)
    );


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


    messageElement.classList.add(
        "message",
        type
    );


    // ====================================
    // STORE MESSAGE ID
    // ====================================

    if (messageId) {

        messageElement.dataset.messageId =
            messageId;
    }


    // ====================================
    // MESSAGE TEXT
    // ====================================

    const textElement =
        document.createElement("span");

    textElement.textContent =
        message;


    // ====================================
    // MESSAGE TIME
    // ====================================

    const timeElement =
        document.createElement("small");

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

        if (messageStatus === "delivered") {

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
    // ADD TO CHAT
    // ====================================

    messages.appendChild(
        messageElement
    );


    // Scroll to latest message

    messages.scrollTop =
        messages.scrollHeight;
}


// ========================================
// UPDATE MESSAGE STATUS
// ========================================

function updateMessageStatus(
    messageId,
    messageStatus
) {

    console.log(
        "Updating message:",
        messageId,
        "to:",
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
            "Status element not found"
        );

        return;
    }


    if (messageStatus === "delivered") {

        statusElement.textContent =
            "✓✓";
    }
}


// ========================================
// SEND BUTTON
// ========================================

sendButton.addEventListener(
    "click",
    sendMessage
);


// ========================================
// ENTER KEY
// ========================================

messageInput.addEventListener(
    "keydown",
    function (event) {

        if (event.key === "Enter") {

            sendMessage();
        }
    }
);


// ========================================
// LOAD HISTORY WHEN PAGE OPENS
// ========================================

loadChatHistory();