document.addEventListener('DOMContentLoaded', function() {
    const chatMessages = document.getElementById('chat-messages');
    const userInput = document.getElementById('user-input');
    const sendButton = document.getElementById('send-button');
    const clearButton = document.getElementById('clear-chat');

    const userMessageTemplate = document.getElementById('user-message-template');
    const botMessageTemplate = document.getElementById('bot-message-template');
    const dataTableTemplate = document.getElementById('data-table-template');
    const typingIndicatorTemplate = document.getElementById('typing-indicator-template');

    userInput.focus();

    function sendMessage() {
        const message = userInput.value.trim();
        if (message.length === 0) return;

        addUserMessage(message);

        userInput.value = '';

        const typingIndicator = showTypingIndicator();

        fetch('/api/chat', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ message })
        })
        .then(response => response.json())
        .then(data => {
            chatMessages.removeChild(typingIndicator);

            addBotResponse(data);
        })
        .catch(error => {
            chatMessages.removeChild(typingIndicator);

            addBotErrorMessage("Sorry, I couldn't process your request. Please try again.");
            console.error('Error:', error);
        });
    }

    function addUserMessage(message) {
        const messageElement = userMessageTemplate.content.cloneNode(true);
        messageElement.querySelector('.message-content').textContent = message;
        messageElement.querySelector('.message-time').textContent = getCurrentTime();
        chatMessages.appendChild(messageElement);
        scrollToBottom();
    }

    function addBotResponse(data) {
    const messageElement = botMessageTemplate.content.cloneNode(true);
    const messageContent = messageElement.querySelector('.message-content');

    if (data.response) {
        const responseText = document.createElement('p');
        responseText.textContent = data.response;
        messageContent.appendChild(responseText);
    }

    if (data.entity_data && Array.isArray(data.entity_data)) {
        data.entity_data.forEach(tableInfo => {
            if (tableInfo.table_name) {
                const tableHeader = document.createElement('h4');
                tableHeader.textContent = tableInfo.table_name;
                messageContent.appendChild(tableHeader);
            }

            if (tableInfo.rows && tableInfo.rows.length > 0) {
                addDataTable(messageContent, tableInfo.rows);
            }
        });
    }
    else if (data.data) {
        if (Array.isArray(data.data)) {
            addDataTable(messageContent, data.data);
        } else {
            addDataTable(messageContent, [data.data]);
        }
    }

    messageElement.querySelector('.message-time').textContent = getCurrentTime();

    chatMessages.appendChild(messageElement);
    scrollToBottom();
}

    function addBotErrorMessage(errorMessage) {
        const messageElement = botMessageTemplate.content.cloneNode(true);
        messageElement.querySelector('.message-content').textContent = errorMessage;
        messageElement.querySelector('.message-content').style.color = '#e74c3c';
        messageElement.querySelector('.message-time').textContent = getCurrentTime();
        chatMessages.appendChild(messageElement);
        scrollToBottom();
    }

    function showTypingIndicator() {
        const indicator = typingIndicatorTemplate.content.cloneNode(true);
        chatMessages.appendChild(indicator);
        scrollToBottom();
        return chatMessages.lastElementChild;
    }

    function addDataTable(container, data) {
        if (!data || data.length === 0) return;

        const tableWrapper = document.createElement('div');
        tableWrapper.className = 'data-table-wrapper';

        const table = dataTableTemplate.content.cloneNode(true).querySelector('table');

        const headers = Object.keys(data[0]);

        const headerRow = table.querySelector('thead tr');
        headers.forEach(header => {
            const th = document.createElement('th');
            th.textContent = formatHeaderName(header);
            headerRow.appendChild(th);
        });

        const tbody = table.querySelector('tbody');
        data.forEach(item => {
            const row = document.createElement('tr');

            headers.forEach(header => {
                const td = document.createElement('td');
                const value = item[header];

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

    function formatHeaderName(header) {
        return header
            .split('_')
            .map(word => word.charAt(0).toUpperCase() + word.slice(1))
            .join(' ');
    }

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

    function scrollToBottom() {
        chatMessages.scrollTop = chatMessages.scrollHeight;
    }

    function clearChat() {
        while (chatMessages.children.length > 1) {
            chatMessages.removeChild(chatMessages.lastChild);
        }
        userInput.focus();
    }

    sendButton.addEventListener('click', sendMessage);

    userInput.addEventListener('keypress', function(e) {
        if (e.key === 'Enter') {
            sendMessage();
        }
    });

    clearButton.addEventListener('click', clearChat);

    const exampleQueries = [
        {
            query: "Show me markets",
            description: "Basic view with only essential fields"
        },
        {
            query: "Show me all details about markets",
            description: "Detailed view with all available fields"
        },
        {
            query: "Show me brokers with their contact details",
            description: "Query with sensitive encrypted fields"
        },
        {
            query: "Show me the highest priced assets",
            description: "Comparative query with ranking"
        },
        {
            query: "Show me recent trades in New York markets",
            description: "Complex query with filtering and relationships"
        }
    ];

    function createExampleQuestions() {
        const exampleContainer = document.createElement('div');
        exampleContainer.className = 'message bot';

        const exampleContent = document.createElement('div');
        exampleContent.className = 'message-content';
        exampleContent.innerHTML = '<p>Try one of these example queries:</p>';

        const questionsList = document.createElement('div');
        questionsList.className = 'example-questions';

        exampleQueries.forEach(example => {
            const questionButton = document.createElement('button');
            questionButton.className = 'example-question-btn';

            const queryContainer = document.createElement('div');

            const queryText = document.createElement('div');
            queryText.className = 'query-text';
            queryText.textContent = example.query;
            queryContainer.appendChild(queryText);

            const queryDescription = document.createElement('div');
            queryDescription.className = 'query-description';
            queryDescription.textContent = example.description;
            queryContainer.appendChild(queryDescription);

            questionButton.appendChild(queryContainer);

            questionButton.addEventListener('click', () => {
                userInput.value = example.query;
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

    setTimeout(() => {
        createExampleQuestions();
        scrollToBottom();
    }, 1000);
});