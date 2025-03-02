document.addEventListener('DOMContentLoaded', function() {
    const chatMessages = document.getElementById('chat-messages');
    const userInput = document.getElementById('user-input');
    const sendButton = document.getElementById('send-button');
    const clearButton = document.getElementById('clear-chat');

    // Templates
    const userMessageTemplate = document.getElementById('user-message-template');
    const botMessageTemplate = document.getElementById('bot-message-template');
    const dataTableTemplate = document.getElementById('data-table-template');
    const typingIndicatorTemplate = document.getElementById('typing-indicator-template');

    // Focus input on load
    userInput.focus();

    // Send message function
    function sendMessage() {
        const message = userInput.value.trim();
        if (message.length === 0) return;

        // Add user message to chat
        addUserMessage(message);

        // Clear input
        userInput.value = '';

        // Show typing indicator
        const typingIndicator = showTypingIndicator();

        // Send message to server
        fetch('/api/chat', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ message })
        })
        .then(response => response.json())
        .then(data => {
            // Remove typing indicator
            chatMessages.removeChild(typingIndicator);

            // Add bot response to chat
            addBotResponse(data);
        })
        .catch(error => {
            // Remove typing indicator
            chatMessages.removeChild(typingIndicator);

            // Show error message
            addBotErrorMessage("Sorry, I couldn't process your request. Please try again.");
            console.error('Error:', error);
        });
    }

    // Add user message to chat
    function addUserMessage(message) {
        const messageElement = userMessageTemplate.content.cloneNode(true);
        messageElement.querySelector('.message-content').textContent = message;
        messageElement.querySelector('.message-time').textContent = getCurrentTime();
        chatMessages.appendChild(messageElement);
        scrollToBottom();
    }

    // Add bot response to chat
    function addBotResponse(data) {
        const messageElement = botMessageTemplate.content.cloneNode(true);
        const messageContent = messageElement.querySelector('.message-content');

        // Add text response
        if (data.response) {
            messageContent.textContent = data.response;
        }

        // Check if there's data to display in a table
        if (data.data) {
            if (Array.isArray(data.data)) {
                // It's an array of objects
                addDataTable(messageContent, data.data);
            } else {
                // It's a single object
                addDataTable(messageContent, [data.data]);
            }
        }

        // Add timestamp
        messageElement.querySelector('.message-time').textContent = getCurrentTime();

        // Add message to chat
        chatMessages.appendChild(messageElement);
        scrollToBottom();
    }

    // Add error message
    function addBotErrorMessage(errorMessage) {
        const messageElement = botMessageTemplate.content.cloneNode(true);
        messageElement.querySelector('.message-content').textContent = errorMessage;
        messageElement.querySelector('.message-content').style.color = '#e74c3c';
        messageElement.querySelector('.message-time').textContent = getCurrentTime();
        chatMessages.appendChild(messageElement);
        scrollToBottom();
    }

    // Show typing indicator
    function showTypingIndicator() {
        const indicator = typingIndicatorTemplate.content.cloneNode(true);
        chatMessages.appendChild(indicator);
        scrollToBottom();
        return chatMessages.lastElementChild;
    }

    // Add data table to message
    function addDataTable(container, data) {
        if (!data || data.length === 0) return;

        // Create wrapper for scrollable table
        const tableWrapper = document.createElement('div');
        tableWrapper.className = 'data-table-wrapper';

        // Clone table template
        const table = dataTableTemplate.content.cloneNode(true).querySelector('table');

        // Get headers from first object
        const headers = Object.keys(data[0]);

        // Create header row
        const headerRow = table.querySelector('thead tr');
        headers.forEach(header => {
            const th = document.createElement('th');
            th.textContent = formatHeaderName(header);
            headerRow.appendChild(th);
        });

        // Create data rows
        const tbody = table.querySelector('tbody');
        data.forEach(item => {
            const row = document.createElement('tr');

            headers.forEach(header => {
                const td = document.createElement('td');
                const value = item[header];

                // Handle encrypted data
                if (typeof value === 'string' && value.startsWith('[ENCRYPTED:')) {
                    const encryptedSpan = document.createElement('span');
                    encryptedSpan.className = 'encrypted-data';
                    encryptedSpan.textContent = '[ENCRYPTED]';
                    td.appendChild(encryptedSpan);
                } else {
                    td.textContent = value;
                }

                row.appendChild(td);
            });

            tbody.appendChild(row);
        });

        tableWrapper.appendChild(table);
        container.appendChild(tableWrapper);
    }

    // Format header name for display
    function formatHeaderName(header) {
        // Convert snake_case to Title Case
        return header
            .split('_')
            .map(word => word.charAt(0).toUpperCase() + word.slice(1))
            .join(' ');
    }

    // Get current time as string
    function getCurrentTime() {
        const now = new Date();
        let hours = now.getHours();
        let minutes = now.getMinutes();
        const ampm = hours >= 12 ? 'PM' : 'AM';

        hours = hours % 12;
        hours = hours ? hours : 12;
        minutes = minutes < 10 ? '0' + minutes : minutes;

        return `${hours}:${minutes} ${ampm}`;
    }

    // Scroll to bottom of chat
    function scrollToBottom() {
        chatMessages.scrollTop = chatMessages.scrollHeight;
    }

    // Clear chat history
    function clearChat() {
        // Keep only the welcome message
        while (chatMessages.children.length > 1) {
            chatMessages.removeChild(chatMessages.lastChild);
        }
        userInput.focus();
    }

    // Event listeners
    sendButton.addEventListener('click', sendMessage);

    userInput.addEventListener('keypress', function(e) {
        if (e.key === 'Enter') {
            sendMessage();
        }
    });

    clearButton.addEventListener('click', clearChat);

    // Add some example questions as buttons
    const exampleQuestions = [
        "What markets are available?",
        "Show me all brokers",
        "What types of assets do we have?",
        "Show me recent trades",
        "Which orders are completed?"
    ];

    // Create example questions UI
    function createExampleQuestions() {
        const exampleContainer = document.createElement('div');
        exampleContainer.className = 'message bot';

        const exampleContent = document.createElement('div');
        exampleContent.className = 'message-content';
        exampleContent.innerHTML = '<p>Try asking one of these questions:</p>';

        const questionsList = document.createElement('div');
        questionsList.className = 'example-questions';

        exampleQuestions.forEach(question => {
            const questionButton = document.createElement('button');
            questionButton.className = 'example-question-btn';
            questionButton.textContent = question;
            questionButton.addEventListener('click', () => {
                userInput.value = question;
                sendMessage();
            });
            questionsList.appendChild(questionButton);
        });

        exampleContent.appendChild(questionsList);
        exampleContainer.appendChild(exampleContent);

        const timeElement = document.createElement('div');
        timeElement.className = 'message-time';
        timeElement.textContent = getCurrentTime();
        exampleContainer.appendChild(timeElement);

        chatMessages.appendChild(exampleContainer);
    }

    // Add additional styles for example questions
    function addExampleQuestionsStyles() {
        const style = document.createElement('style');
        style.textContent = `
            .example-questions {
                display: flex;
                flex-direction: column;
                gap: 8px;
                margin-top: 10px;
            }
            
            .example-question-btn {
                background-color: #f0f0f0;
                border: 1px solid #ddd;
                border-radius: 16px;
                padding: 8px 12px;
                text-align: left;
                cursor: pointer;
                transition: background-color 0.2s;
                font-size: 14px;
                color: #0084ff;
            }
            
            .example-question-btn:hover {
                background-color: #e4e6eb;
            }
        `;
        document.head.appendChild(style);
    }

    // Add example questions after a short delay
    setTimeout(() => {
        addExampleQuestionsStyles();
        createExampleQuestions();
        scrollToBottom();
    }, 1000);
});