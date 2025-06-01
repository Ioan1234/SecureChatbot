
class SpeechRecognitionHandler {
    constructor(options = {}) {
        this.isListening = false;
        this.continuous = options.continuous || false;
        this.interimResults = options.interimResults || true;
        this.lang = options.lang || 'en-US';
        this.maxAlternatives = options.maxAlternatives || 1;
        this.onStartCallback = options.onStart || (() => {});
        this.onEndCallback = options.onEnd || (() => {});
        this.onResultCallback = options.onResult || ((result) => console.log('Speech result:', result));
        this.onErrorCallback = options.onError || ((error) => console.error('Speech error:', error));
        this.useServerProcessing = options.useServerProcessing || false;
        this.audioChunks = [];
        this.mediaRecorder = null;

        if (!('webkitSpeechRecognition' in window) && !('SpeechRecognition' in window)) {
            console.error('Speech recognition not supported in this browser');
            this.supported = false;
            return;
        }

        this.supported = true;
        this.recognition = new (window.SpeechRecognition || window.webkitSpeechRecognition)();
        this.recognition.continuous = this.continuous;
        this.recognition.interimResults = this.interimResults;
        this.recognition.lang = this.lang;
        this.recognition.maxAlternatives = this.maxAlternatives;

        this._setupRecognitionHandlers();
    }

    _setupRecognitionHandlers() {
        this.recognition.onstart = () => {
            this.isListening = true;
            this.onStartCallback();
        };

        this.recognition.onend = () => {
            this.isListening = false;
            this.onEndCallback();
        };

        this.recognition.onresult = (event) => {
            const transcript = Array.from(event.results)
                .map(result => result[0])
                .map(result => result.transcript)
                .join('');

            const isFinal = event.results[0].isFinal;

            this.onResultCallback({
                transcript,
                isFinal
            });
        };

        this.recognition.onerror = (event) => {
            this.isListening = false;
            this.onErrorCallback(event.error);
        };
    }

    startListening() {
        if (!this.supported) {
            this.onErrorCallback('Speech recognition not supported in this browser');
            return;
        }

        if (this.isListening) return;

        try {
            this.recognition.start();

            if (this.useServerProcessing) {
                this._startAudioRecording();
            }
        } catch (error) {
            console.error('Error starting speech recognition:', error);
            this.onErrorCallback(error);
        }
    }

    stopListening() {
        if (!this.supported || !this.isListening) return;

        try {
            this.recognition.stop();

            if (this.useServerProcessing && this.mediaRecorder) {
                this.mediaRecorder.stop();
            }
        } catch (error) {
            console.error('Error stopping speech recognition:', error);
        }
    }

    async _startAudioRecording() {
        try {
            const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
            this.audioChunks = [];

            this.mediaRecorder = new MediaRecorder(stream);

            this.mediaRecorder.ondataavailable = (event) => {
                if (event.data.size > 0) {
                    this.audioChunks.push(event.data);
                }
            };

            this.mediaRecorder.onstop = () => {
                const audioBlob = new Blob(this.audioChunks, { type: 'audio/wav' });
                this._sendAudioToServer(audioBlob);

                stream.getTracks().forEach(track => track.stop());
            };

            this.mediaRecorder.start();
        } catch (error) {
            console.error('Error starting audio recording:', error);
            this.onErrorCallback(error);
        }
    }

    _sendAudioToServer(audioBlob) {
        const formData = new FormData();
        formData.append('audio', audioBlob, 'speech.wav');

        fetch('/api/speech_recognition', {
            method: 'POST',
            body: formData
        })
        .then(response => response.json())
        .then(data => {
            console.log('Server response:', data);
            if (data.transcript) {
                const userInput = document.getElementById('user-input');
                if (userInput) {
                    userInput.value = data.transcript;
                }

                const sendButton = document.getElementById('send-button');
                if (sendButton && userInput.value.trim() !== '') {
                    setTimeout(() => {
                        sendButton.click();
                    }, 500);
                }
            }
        })
        .catch(error => {
            console.error('Error sending audio to server:', error);
            this.onErrorCallback(error);
        });
    }
}

document.addEventListener('DOMContentLoaded', () => {
  const btn = document.getElementById('speech-button');
  const userInput = document.getElementById('user-input');
  const sendBtn = document.getElementById('send-button');

  const speech = new SpeechRecognitionHandler({
    useServerProcessing: true,
    onStart:   () => btn.classList.add('listening'),
    onEnd:     () => btn.classList.remove('listening'),
    onResult:  ({ transcript, isFinal }) => {
      userInput.value = transcript;
      if (isFinal && transcript.trim()) {
        sendBtn.click();
      }
    },
    onError:   err => {
      console.error('Speech error:', err);
    }
  });

  btn.addEventListener('mousedown', () => speech.startListening());
  btn.addEventListener('mouseup',   () => speech.stopListening());
});



document.addEventListener('DOMContentLoaded', function() {
    const chatInput = document.querySelector('.chat-input');
    const sendButton = document.getElementById('send-button');

    if (!chatInput || !sendButton || document.getElementById('speech-button')) {
        console.log('Chat input, send button not found, or speech button already exists');
        return;
    }

    const speechButton = document.createElement('button');
    speechButton.id = 'speech-button';
    speechButton.innerHTML = '<i class="fas fa-microphone"></i>';
    speechButton.style.border = 'none';
    speechButton.style.backgroundColor = 'transparent';
    speechButton.style.marginLeft = '10px';
    speechButton.style.color = '#0084ff';
    speechButton.style.fontSize = '20px';
    speechButton.style.cursor = 'pointer';
    speechButton.style.width = '40px';
    speechButton.style.height = '40px';
    speechButton.style.borderRadius = '50%';
    speechButton.style.display = 'flex';
    speechButton.style.justifyContent = 'center';
    speechButton.style.alignItems = 'center';

    chatInput.insertBefore(speechButton, sendButton);


    const statusIndicator = document.createElement('div');
    statusIndicator.id = 'speech-status';
    statusIndicator.textContent = 'Listening...';
    statusIndicator.style.position = 'absolute';
    statusIndicator.style.top = '-30px';
    statusIndicator.style.left = '50%';
    statusIndicator.style.transform = 'translateX(-50%)';
    statusIndicator.style.backgroundColor = 'rgba(0, 0, 0, 0.7)';
    statusIndicator.style.color = 'white';
    statusIndicator.style.padding = '5px 10px';
    statusIndicator.style.borderRadius = '15px';
    statusIndicator.style.fontSize = '12px';
    statusIndicator.style.opacity = '0';
    statusIndicator.style.transition = 'opacity 0.3s';

    chatInput.style.position = 'relative';
    chatInput.appendChild(statusIndicator);

    if (!('webkitSpeechRecognition' in window) && !('SpeechRecognition' in window)) {
        console.error('Speech recognition not supported');
        speechButton.disabled = true;
        speechButton.title = 'Speech recognition not supported in this browser';
        return;
    }

    const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
    const recognition = new SpeechRecognition();

    recognition.continuous = false;
    recognition.interimResults = true;
    recognition.lang = 'en-US';

    let isListening = false;

    recognition.onstart = function() {
        isListening = true;
        speechButton.style.color = '#e74c3c';
        statusIndicator.style.opacity = '1';
        console.log('Speech recognition started');

        speechButton.animate([
            { transform: 'scale(1)' },
            { transform: 'scale(1.1)' },
            { transform: 'scale(1)' }
        ], {
            duration: 1500,
            iterations: Infinity
        });
    };

    recognition.onend = function() {
        isListening = false;
        speechButton.style.color = '#0084ff';
        statusIndicator.style.opacity = '0';
        console.log('Speech recognition ended');

        speechButton.getAnimations().forEach(animation => animation.cancel());
    };

    recognition.onresult = function(event) {
        const transcript = Array.from(event.results)
            .map(result => result[0])
            .map(result => result.transcript)
            .join('');

        console.log('Recognized:', transcript);

        const userInput = document.getElementById('user-input');
        if (userInput) {
            userInput.value = transcript;
        }

        if (event.results[0].isFinal) {
            console.log('Final result - preparing to submit');
            setTimeout(() => {
                if (userInput.value.trim() !== '') {
                    console.log('Auto-submitting: ', userInput.value);

                    sendButton.click();

                    setTimeout(() => {
                        const clickEvent = new MouseEvent('click', {
                            bubbles: true,
                            cancelable: true,
                            view: window
                        });
                        sendButton.dispatchEvent(clickEvent);
                    }, 100);

                    setTimeout(() => {
                        if (typeof window.sendMessage === 'function') {
                            window.sendMessage();
                        }
                    }, 200);
                }
            }, 1000);
        }
    };

    recognition.onerror = function(event) {
        console.error('Speech recognition error:', event.error);
        isListening = false;
        speechButton.style.color = '#0084ff';
        statusIndicator.style.opacity = '0';

        speechButton.getAnimations().forEach(animation => animation.cancel());

        const chatMessages = document.getElementById('chat-messages');
        if (chatMessages) {
            const errorTemplate = document.getElementById('bot-message-template');
            if (errorTemplate) {
                const messageElement = errorTemplate.content.cloneNode(true);
                messageElement.querySelector('.message-content').textContent =
                    `Speech recognition error: ${event.error}. Please try again or type your message.`;
                chatMessages.appendChild(messageElement);
                chatMessages.scrollTop = chatMessages.scrollHeight;
            }
        }
    };

    speechButton.addEventListener('click', function() {
        if (isListening) {
            recognition.stop();
        } else {
            recognition.start();
        }
    });

    document.addEventListener('keydown', (e) => {
        if (e.ctrlKey && e.shiftKey && e.code === 'Space') {
            e.preventDefault();
            if (isListening) {
                recognition.stop();
            } else {
                recognition.start();
            }
        }
    });

    try {
        const sendButtonClickHandlers = getEventListeners(sendButton).click;
        if (sendButtonClickHandlers && sendButtonClickHandlers.length > 0) {
            window.sendMessage = sendButtonClickHandlers[0].listener;
            console.log('Found send message function');
        }
    } catch (error) {
        console.log('Could not access event listeners (normal in production):', error);
    }
});