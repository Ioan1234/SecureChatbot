# speech/speech_recognition.py
import logging
import numpy as np
import os
import tempfile
from typing import Dict, Any, Optional, Tuple, List

# Global flag to track if dependencies are available
SPEECH_DEPENDENCIES_AVAILABLE = False

# Try to import speech recognition libraries
try:
    import librosa
    import soundfile as sf
    import speech_recognition as sr
    from pydub import AudioSegment

    SPEECH_DEPENDENCIES_AVAILABLE = True
except ImportError as e:
    logging.error(f"Speech recognition libraries not installed: {e}")
    logging.error("Run: pip install librosa soundfile SpeechRecognition pydub")


    # Create dummy sr class to prevent sr is not defined errors
    class DummySR:
        class Recognizer:
            def __init__(self):
                pass

        class Microphone:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                pass

        class AudioFile:
            def __init__(self, filename):
                self.filename = filename

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                pass

        class UnknownValueError(Exception):
            pass

        class RequestError(Exception):
            pass


    sr = DummySR()


class SecureSpeechRecognition:
    """
    Speech recognition module with homomorphic encryption support
    for secure audio processing and voice command recognition.
    """

    def __init__(self, encryption_manager=None, model_path=None, use_encryption=True):
        """
        Initialize the secure speech recognition module

        Args:
            encryption_manager: HomomorphicEncryptionManager instance for secure processing
            model_path: Path to the speech recognition model (if using a custom model)
            use_encryption: Whether to use encryption for audio feature processing
        """
        self.logger = logging.getLogger(__name__)

        # Check if dependencies are available
        if not SPEECH_DEPENDENCIES_AVAILABLE:
            self.logger.error("Speech recognition dependencies not available")
            raise ImportError(
                "Speech recognition dependencies not installed. Run: pip install librosa soundfile SpeechRecognition pydub")

        self.encryption_manager = encryption_manager
        self.model_path = model_path
        self.use_encryption = use_encryption

        # Initialize speech recognizer
        self.recognizer = sr.Recognizer()

        # Configure speech recognizer
        self.recognizer.energy_threshold = 300  # Minimum audio energy to consider for recording
        self.recognizer.dynamic_energy_threshold = True
        self.recognizer.dynamic_energy_adjustment_damping = 0.15
        self.recognizer.pause_threshold = 0.8  # Seconds of non-speaking audio before a phrase is considered complete

        # Feature extraction parameters
        self.sample_rate = 16000
        self.n_mfcc = 13  # Number of MFCC features to extract

        self.logger.info("Secure speech recognition module initialized")

    def record_audio(self, duration=5, source=None):
        """
        Record audio from microphone

        Args:
            duration: Recording duration in seconds
            source: Optional audio source (if None, uses microphone)

        Returns:
            Audio data as numpy array
        """
        try:
            # Use provided source or default to microphone
            if source is None:
                self.logger.info("Recording from microphone...")
                with sr.Microphone() as source:
                    # Adjust for ambient noise
                    self.recognizer.adjust_for_ambient_noise(source, duration=0.5)
                    audio = self.recognizer.record(source, duration=duration)
            else:
                self.logger.info("Recording from provided source...")
                audio = self.recognizer.record(source, duration=duration)

            # Convert audio data to numpy array
            audio_data = np.frombuffer(audio.frame_data, dtype=np.int16)
            return audio_data

        except Exception as e:
            self.logger.error(f"Error recording audio: {e}")
            return None

    def load_audio_file(self, file_path):
        """
        Load audio from file

        Args:
            file_path: Path to audio file

        Returns:
            Audio data as numpy array and sample rate
        """
        try:
            # Use librosa to load audio file
            audio_data, sample_rate = librosa.load(file_path, sr=self.sample_rate)
            return audio_data, sample_rate

        except Exception as e:
            self.logger.error(f"Error loading audio file: {e}")
            return None, None

    def extract_features(self, audio_data, sample_rate=None):
        """
        Extract MFCC features from audio data

        Args:
            audio_data: Audio data as numpy array
            sample_rate: Sample rate of audio data

        Returns:
            MFCC features as numpy array
        """
        try:
            if sample_rate is None:
                sample_rate = self.sample_rate

            # Extract MFCC features
            mfcc_features = librosa.feature.mfcc(
                y=audio_data,
                sr=sample_rate,
                n_mfcc=self.n_mfcc
            )

            return mfcc_features

        except Exception as e:
            self.logger.error(f"Error extracting features: {e}")
            return None

    def encrypt_features(self, features):
        """
        Encrypt audio features using homomorphic encryption

        Args:
            features: Audio features to encrypt

        Returns:
            Encrypted features
        """
        if not self.encryption_manager or not self.use_encryption:
            self.logger.warning(
                "Encryption manager not available or encryption disabled. Returning unencrypted features.")
            return features

        try:
            # Flatten features for encryption
            flattened_features = features.flatten()

            # Encrypt features
            encrypted_features = self.encryption_manager.encrypt_vector(flattened_features)

            return encrypted_features

        except Exception as e:
            self.logger.error(f"Error encrypting features: {e}")
            return None

    def decrypt_features(self, encrypted_features):
        """
        Decrypt encrypted audio features

        Args:
            encrypted_features: Encrypted audio features

        Returns:
            Decrypted features
        """
        if not self.encryption_manager or not self.use_encryption:
            self.logger.warning("Encryption manager not available or encryption disabled. Returning input as is.")
            return encrypted_features

        try:
            # Decrypt features
            decrypted_features = self.encryption_manager.decrypt_vector(encrypted_features)

            return decrypted_features

        except Exception as e:
            self.logger.error(f"Error decrypting features: {e}")
            return None

    def recognize_speech(self, audio_data=None, audio_file=None):
        """
        Recognize speech from audio data or file

        Args:
            audio_data: Audio data as numpy array (optional)
            audio_file: Path to audio file (optional)

        Returns:
            Recognized text or None if recognition failed
        """
        try:
            audio = None

            # Use provided audio data or file
            if audio_data is not None:
                # Create temporary WAV file
                with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp_file:
                    tmp_path = tmp_file.name

                # Save audio data to temporary WAV file
                sf.write(tmp_path, audio_data, self.sample_rate)

                # Load audio from temporary file
                with sr.AudioFile(tmp_path) as source:
                    audio = self.recognizer.record(source)

                # Remove temporary file
                os.remove(tmp_path)

            elif audio_file is not None:
                # Load audio from file
                with sr.AudioFile(audio_file) as source:
                    audio = self.recognizer.record(source)

            else:
                self.logger.error("No audio data or file provided")
                return None

            # Recognize speech
            recognized_text = self.recognizer.recognize_google(audio)
            self.logger.info(f"Recognized text: {recognized_text}")

            return recognized_text

        except sr.UnknownValueError:
            self.logger.warning("Speech recognition could not understand audio")
            return None

        except sr.RequestError as e:
            self.logger.error(f"Could not request results from speech recognition service: {e}")
            return None

        except Exception as e:
            self.logger.error(f"Error recognizing speech: {e}")
            return None

    def secure_process_audio(self, audio_data=None, audio_file=None):
        """
        Process audio data or file with encryption

        Args:
            audio_data: Audio data as numpy array (optional)
            audio_file: Path to audio file (optional)

        Returns:
            Recognized text or None if processing failed
        """
        try:
            # Load audio data
            if audio_data is None and audio_file is not None:
                audio_data, sample_rate = self.load_audio_file(audio_file)
            elif audio_data is None:
                self.logger.error("No audio data or file provided")
                return None

            # Extract features
            features = self.extract_features(audio_data)

            # Process features
            if self.use_encryption and self.encryption_manager:
                # Encrypt features
                self.logger.info("Encrypting audio features...")
                encrypted_features = self.encrypt_features(features)

                # Here you could send encrypted features to a secure server for processing
                # For demo purposes, we'll decrypt them locally
                decrypted_features = self.decrypt_features(encrypted_features)

                # Reshape features (simplified - in practice more complex reconstruction might be needed)
                if decrypted_features is not None:
                    features = np.array(decrypted_features).reshape(self.n_mfcc, -1)

            # Recognize speech from original audio
            # Note: For truly secure processing, you would need to implement a model
            # that works with encrypted features. This is a simplification.
            recognized_text = self.recognize_speech(audio_data)

            return recognized_text

        except Exception as e:
            self.logger.error(f"Error in secure audio processing: {e}")
            return None

    def start_voice_command_mode(self, chatbot_engine, duration=5):
        """
        Start voice command mode to listen for commands

        Args:
            chatbot_engine: ChatbotEngine instance to process recognized commands
            duration: Duration to listen for each command in seconds

        Returns:
            None
        """
        self.logger.info("Starting voice command mode...")
        print("Listening for voice commands. Speak now...")

        try:
            with sr.Microphone() as source:
                # Adjust for ambient noise
                self.recognizer.adjust_for_ambient_noise(source, duration=1)

                while True:
                    print("\nListening... (say 'exit' to quit)")

                    # Listen for command
                    audio = self.recognizer.listen(source, timeout=duration)

                    try:
                        # Recognize speech
                        command = self.recognizer.recognize_google(audio)
                        print(f"You said: {command}")

                        # Check for exit command
                        if command.lower() in ["exit", "quit", "goodbye", "bye"]:
                            print("Exiting voice command mode.")
                            break

                        # Process command with chatbot engine
                        if chatbot_engine:
                            result = chatbot_engine.process_user_input(command)
                            # Print chatbot response
                            if result and "response" in result:
                                print(f"Chatbot: {result['response']}")

                    except sr.UnknownValueError:
                        print("Sorry, I didn't understand that.")

                    except sr.RequestError as e:
                        print(f"Could not request results; {e}")

        except KeyboardInterrupt:
            print("\nVoice command mode stopped.")

        except Exception as e:
            self.logger.error(f"Error in voice command mode: {e}")
            print(f"Error: {e}")