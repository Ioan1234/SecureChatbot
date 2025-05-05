document.addEventListener('DOMContentLoaded', function() {
    const chatMessages = document.getElementById('chat-messages');
    const userInput = document.getElementById('user-input');
    const sendButton = document.getElementById('send-button');
    const clearButton = document.getElementById('clear-chat');
    const sqlModeButton = document.getElementById('sql-mode');

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

        if (document.querySelector('.chat-input').classList.contains('sql-mode')) {
            chatMessages.removeChild(typingIndicator);
            executeSqlQuery(message);
            return;
        }

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

        if (data.data || data.entity_data) {
            const exportButton = document.createElement('button');
            exportButton.className = 'export-data-btn';
            exportButton.innerHTML = '<i class="fas fa-download"></i> Export as CSV';
            exportButton.addEventListener('click', () => {
                exportDataToCSV(data.data || getDataFromEntityData(data.entity_data));
            });
            messageContent.appendChild(exportButton);
        }

        if (data.suggestions) {
            addSuggestions(messageContent, data.suggestions);
        } else if (data.data && Array.isArray(data.data) && data.data.length > 0) {
            const suggestions = generateSuggestions(data.data);
            if (suggestions.length > 0) {
                addSuggestions(messageContent, suggestions);
            }
        }

        messageElement.querySelector('.message-time').textContent = getCurrentTime();

        chatMessages.appendChild(messageElement);
        scrollToBottom();
    }

    function addSuggestions(container, suggestions) {
        const suggestionList = document.createElement('ul');
        suggestionList.className = 'suggestion-list';

        suggestions.forEach(suggestion => {
            const li = document.createElement('li');
            const button = document.createElement('button');
            button.className = 'suggestion-btn';
            button.textContent = suggestion;
            button.addEventListener('click', () => {
                userInput.value = suggestion;
                sendMessage();
            });
            li.appendChild(button);
            suggestionList.appendChild(li);
        });

        container.appendChild(suggestionList);
    }

    function generateSuggestions(data) {
      const suggestions = [];
      if (!data || data.length === 0) return suggestions;

      const cols = Object.keys(data[0]);

      if (cols.includes('account_balance')) {
        suggestions.push("List top 5 traders by account balance");
        suggestions.push("Show traders ranked by highest account balance");
      }
      else if (cols.includes('trade_date')) {
        suggestions.push("Display the newest trading activity");
        suggestions.push("List the most recent transactions in the market");
      }
      else if (cols.includes('asset_type')) {
        suggestions.push("List all bond assets");
        suggestions.push("Show all ETF assets");
      }
      else if (cols.includes('trade_count') || cols.includes('num_trades')) {
        suggestions.push("Find the trader with the highest number of trades");
        suggestions.push("Identify which user trades the most frequently");
      }

      if (suggestions.length === 0) {
        suggestions.push("Show current asset prices");
        suggestions.push("List account types and their respective counts");
      }

      return suggestions.slice(0, 3);
    }


    function getDataFromEntityData(entityData) {
        let allRows = [];
        if (entityData && Array.isArray(entityData)) {
            entityData.forEach(tableInfo => {
                if (tableInfo.rows && tableInfo.rows.length > 0) {
                    allRows = allRows.concat(tableInfo.rows);
                }
            });
        }
        return allRows;
    }

    function addBotErrorMessage(errorMessage) {
        const messageElement = botMessageTemplate.content.cloneNode(true);
        const content = messageElement.querySelector('.message-content');

        const errorDiv = document.createElement('div');
        errorDiv.className = 'error-message';
        errorDiv.textContent = errorMessage;
        content.appendChild(errorDiv);

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

    const state = {
        currentPage: 1,
        itemsPerPage: 10,
        totalItems: data.length,
        totalPages: Math.ceil(data.length / 10)
    };

    function renderCurrentPage() {
        const startIndex = (state.currentPage - 1) * state.itemsPerPage;
        const endIndex = Math.min(startIndex + state.itemsPerPage, data.length);

        tbody.innerHTML = '';

        const currentPageData = data.slice(startIndex, endIndex);

        currentPageData.forEach(item => {
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
                    td.textContent = value !== null && value !== undefined ? value : '';
                }

                row.appendChild(td);
            });

            tbody.appendChild(row);
        });

        updateRecordCount();
    }

    function updateRecordCount() {
        if (recordCountDisplay) {
            const startRecord = (state.currentPage - 1) * state.itemsPerPage + 1;
            const endRecord = Math.min(state.currentPage * state.itemsPerPage, data.length);
            recordCountDisplay.textContent = `Showing ${startRecord}-${endRecord} of ${data.length} records`;
        }
    }

    tableWrapper.appendChild(table);
    container.appendChild(tableWrapper);

    if (data.length > state.itemsPerPage) {
        const paginationContainer = document.createElement('div');
        paginationContainer.className = 'pagination-controls';

        const firstPageBtn = document.createElement('button');
        firstPageBtn.innerHTML = '&laquo; First';
        firstPageBtn.className = 'pagination-btn';
        firstPageBtn.addEventListener('click', () => {
            if (state.currentPage !== 1) {
                state.currentPage = 1;
                renderCurrentPage();
                updatePaginationButtons();
            }
        });

        const prevPageBtn = document.createElement('button');
        prevPageBtn.innerHTML = '&lt; Previous';
        prevPageBtn.className = 'pagination-btn';
        prevPageBtn.addEventListener('click', () => {
            if (state.currentPage > 1) {
                state.currentPage--;
                renderCurrentPage();
                updatePaginationButtons();
            }
        });

        const pageDisplay = document.createElement('span');
        pageDisplay.className = 'pagination-info';

        const nextPageBtn = document.createElement('button');
        nextPageBtn.innerHTML = 'Next &gt;';
        nextPageBtn.className = 'pagination-btn';
        nextPageBtn.addEventListener('click', () => {
            if (state.currentPage < state.totalPages) {
                state.currentPage++;
                renderCurrentPage();
                updatePaginationButtons();
            }
        });

        const lastPageBtn = document.createElement('button');
        lastPageBtn.innerHTML = 'Last &raquo;';
        lastPageBtn.className = 'pagination-btn';
        lastPageBtn.addEventListener('click', () => {
            if (state.currentPage !== state.totalPages) {
                state.currentPage = state.totalPages;
                renderCurrentPage();
                updatePaginationButtons();
            }
        });

        paginationContainer.appendChild(firstPageBtn);
        paginationContainer.appendChild(prevPageBtn);
        paginationContainer.appendChild(pageDisplay);
        paginationContainer.appendChild(nextPageBtn);
        paginationContainer.appendChild(lastPageBtn);

        tableWrapper.appendChild(paginationContainer);

        function updatePaginationButtons() {
            firstPageBtn.disabled = state.currentPage === 1;
            prevPageBtn.disabled = state.currentPage === 1;
            nextPageBtn.disabled = state.currentPage === state.totalPages;
            lastPageBtn.disabled = state.currentPage === state.totalPages;

            pageDisplay.textContent = `Page ${state.currentPage} of ${state.totalPages}`;
        }

        updatePaginationButtons();
    }

    const recordCountDisplay = document.createElement('p');
    recordCountDisplay.className = 'record-count-display';
    tableWrapper.appendChild(recordCountDisplay);

    renderCurrentPage();

    return tableWrapper;
}

function executeSqlQueryWithPagination(sql) {
    const typingIndicator = showTypingIndicator();

    fetch('/api/execute_sql', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ sql })
    })
    .then(response => response.json())
    .then(data => {
        chatMessages.removeChild(typingIndicator);

        const message = botMessageTemplate.content.cloneNode(true);
        const content = message.querySelector('.message-content');

        if (data.error) {
            const errorDiv = document.createElement('div');
            errorDiv.className = 'error-message';
            errorDiv.textContent = data.error;
            content.appendChild(errorDiv);
        } else {
            content.innerHTML = `
                <p>📊 <strong>SQL Results</strong></p>
                <div class="sql-query">${sql}</div>
            `;

            if (data.results && data.results.length > 0) {
                addDataTable(content, data.results);

                const exportButton = document.createElement('button');
                exportButton.className = 'export-data-btn';
                exportButton.innerHTML = '<i class="fas fa-download"></i> Export Results';
                exportButton.addEventListener('click', () => {
                    exportDataToCSV(data.results, 'sql_results');
                });
                content.appendChild(exportButton);
            } else {
                content.innerHTML += '<p>No results returned</p>';
            }
        }

        message.querySelector('.message-time').textContent = getCurrentTime();
        chatMessages.appendChild(message);
        scrollToBottom();
    })
    .catch(error => {
        chatMessages.removeChild(typingIndicator);
        console.error('SQL error:', error);
        addBotErrorMessage("Error executing SQL query.");
    });
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
        const welcomeMessage = chatMessages.firstElementChild;

        while (chatMessages.firstChild) {
            chatMessages.removeChild(chatMessages.firstChild);
        }

        if (welcomeMessage) {
            chatMessages.appendChild(welcomeMessage);
        }

        userInput.focus();
    }

    function toggleSqlMode() {
        const inputArea = document.querySelector('.chat-input');

        if (inputArea.classList.contains('sql-mode')) {
            inputArea.classList.remove('sql-mode');
            userInput.placeholder = "Type a message...";
            sqlModeButton.classList.remove('active');
        } else {
            inputArea.classList.add('sql-mode');
            userInput.placeholder = "Enter SQL query (SELECT only)...";
            sqlModeButton.classList.add('active');

            const sqlMessage = botMessageTemplate.content.cloneNode(true);
            sqlMessage.querySelector('.message-content').innerHTML = `
                <p>🔍 <strong>SQL Mode activated</strong></p>
                <p>You can now enter SQL queries directly. For safety reasons, only SELECT queries are allowed.</p>
                <p class="sql-query">SELECT * FROM traders LIMIT 10</p>
            `;
            sqlMessage.querySelector('.message-time').textContent = getCurrentTime();
            chatMessages.appendChild(sqlMessage);
            scrollToBottom();
        }
    }

    function executeSqlQuery(sql) {
        const typingIndicator = showTypingIndicator();

        fetch('/api/execute_sql', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ sql })
        })
        .then(response => response.json())
        .then(data => {
            chatMessages.removeChild(typingIndicator);

            const message = botMessageTemplate.content.cloneNode(true);
            const content = message.querySelector('.message-content');

            if (data.error) {
                const errorDiv = document.createElement('div');
                errorDiv.className = 'error-message';
                errorDiv.textContent = data.error;
                content.appendChild(errorDiv);
            } else {
                content.innerHTML = `
                    <p>📊 <strong>SQL Results</strong></p>
                    <div class="sql-query">${sql}</div>
                `;

                if (data.results && data.results.length > 0) {
                    addDataTable(content, data.results);

                    const rowInfo = document.createElement('p');
                    rowInfo.style.fontSize = '12px';
                    rowInfo.style.color = '#666';
                    rowInfo.style.fontStyle = 'italic';
                    rowInfo.textContent = `${data.results.length} rows returned`;
                    content.appendChild(rowInfo);

                    const exportButton = document.createElement('button');
                    exportButton.className = 'export-data-btn';
                    exportButton.innerHTML = '<i class="fas fa-download"></i> Export Results';
                    exportButton.addEventListener('click', () => {
                        exportDataToCSV(data.results, 'sql_results');
                    });
                    content.appendChild(exportButton);
                } else {
                    content.innerHTML += '<p>No results returned</p>';
                }
            }

            message.querySelector('.message-time').textContent = getCurrentTime();
            chatMessages.appendChild(message);
            scrollToBottom();
        })
        .catch(error => {
            chatMessages.removeChild(typingIndicator);
            console.error('SQL error:', error);
            addBotErrorMessage("Error executing SQL query.");
        });
    }

    function exportDataToCSV(data, filename = 'export') {
        if (!data || !data.length) {
            alert('No data to export');
            return;
        }

        const headers = Object.keys(data[0]);
        const csvContent = [
            headers.join(','),
            ...data.map(row => {
                return headers.map(header => {
                    const value = row[header];

                    if (value === null || value === undefined) {
                        return '';
                    }

                    if (typeof value === 'string' && (value.includes(',') || value.includes('"') || value.includes('\n'))) {
                        return `"${value.replace(/"/g, '""')}"`;
                    }

                    return value;
                }).join(',');
            })
        ].join('\n');

        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);

        const link = document.createElement('a');
        link.setAttribute('href', url);
        link.setAttribute('download', `${filename}_${getFormattedDate()}.csv`);
        link.style.display = 'none';

        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    }

    function getFormattedDate() {
        const now = new Date();
        const year = now.getFullYear();
        const month = String(now.getMonth() + 1).padStart(2, '0');
        const day = String(now.getDate()).padStart(2, '0');
        const hours = String(now.getHours()).padStart(2, '0');
        const minutes = String(now.getMinutes()).padStart(2, '0');

        return `${year}${month}${day}_${hours}${minutes}`;
    }

    document.querySelectorAll('.suggestion-btn').forEach(button => {
        button.addEventListener('click', function() {
            userInput.value = this.textContent;
            sendMessage();
        });
    });

    sendButton.addEventListener('click', sendMessage);
    userInput.addEventListener('keypress', function(e) {
        if (e.key === 'Enter') {
            sendMessage();
        }
    });
    clearButton.addEventListener('click', clearChat);
    sqlModeButton.addEventListener('click', toggleSqlMode);

    window.sendMessage = sendMessage;
});
