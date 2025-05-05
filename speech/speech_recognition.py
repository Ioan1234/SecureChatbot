import logging
import numpy as np
import os
import tempfile
from typing import Dict, Any, Optional, Tuple, List

SPEECH_DEPENDENCIES_AVAILABLE = False

try:
    import librosa
    import soundfile as sf
    import speech_recognition as sr
    from pydub import AudioSegment

    SPEECH_DEPENDENCIES_AVAILABLE = True
except ImportError as e:
    logging.error(f"Speech recognition libraries not installed: {e}")

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

    def __init__(self, encryption_manager=None, model_path=None, use_encryption=True):

        self.logger = logging.getLogger(__name__)

        if not SPEECH_DEPENDENCIES_AVAILABLE:
            self.logger.error("Speech recognition dependencies not available")
            raise ImportError(
                "Speech recognition dependencies not installed.")

        self.encryption_manager = encryption_manager
        self.model_path = model_path
        self.use_encryption = use_encryption

        self.recognizer = sr.Recognizer()

        self.recognizer.energy_threshold = 300
        self.recognizer.dynamic_energy_threshold = True
        self.recognizer.dynamic_energy_adjustment_damping = 0.15
        self.recognizer.pause_threshold = 0.8

        self.sample_rate = 16000
        self.n_mfcc = 13

        self.logger.info("Secure speech recognition module initialized")

    def record_audio(self, duration=5, source=None):

        try:
            if source is None:
                self.logger.info("Recording from microphone...")
                with sr.Microphone() as source:
                    self.recognizer.adjust_for_ambient_noise(source, duration=0.5)
                    audio = self.recognizer.record(source, duration=duration)
            else:
                self.logger.info("Recording from provided source...")
                audio = self.recognizer.record(source, duration=duration)
            audio_data = np.frombuffer(audio.frame_data, dtype=np.int16)
            return audio_data

        except Exception as e:
            self.logger.error(f"Error recording audio: {e}")
            return None

    def load_audio_file(self, file_path):
        try:
            audio_data, sample_rate = librosa.load(file_path, sr=self.sample_rate)
            return audio_data, sample_rate

        except Exception as e:
            self.logger.error(f"Error loading audio file: {e}")
            return None, None

    def extract_features(self, audio_data, sample_rate=None):
        try:
            if sample_rate is None:
                sample_rate = self.sample_rate
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

        if not self.encryption_manager or not self.use_encryption:
            self.logger.warning(
                "Encryption manager not available or encryption disabled. Returning unencrypted features.")
            return features

        try:
            flattened_features = features.flatten()

            encrypted_features = self.encryption_manager.encrypt_vector(flattened_features)

            return encrypted_features

        except Exception as e:
            self.logger.error(f"Error encrypting features: {e}")
            return None

    def decrypt_features(self, encrypted_features):

        if not self.encryption_manager or not self.use_encryption:
            self.logger.warning("Encryption manager not available or encryption disabled. Returning input as is.")
            return encrypted_features

        try:
            decrypted_features = self.encryption_manager.decrypt_vector(encrypted_features)

            return decrypted_features

        except Exception as e:
            self.logger.error(f"Error decrypting features: {e}")
            return None

    def recognize_speech(self, audio_data=None, audio_file=None):
        try:
            audio = None

            if audio_data is not None:
                with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp_file:
                    tmp_path = tmp_file.name

                sf.write(tmp_path, audio_data, self.sample_rate)

                with sr.AudioFile(tmp_path) as source:
                    audio = self.recognizer.record(source)

                os.remove(tmp_path)

            elif audio_file is not None:
                with sr.AudioFile(audio_file) as source:
                    audio = self.recognizer.record(source)

            else:
                self.logger.error("No audio data or file provided")
                return None

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
        try:

            if audio_data is None and audio_file is not None:
                audio_data, sample_rate = self.load_audio_file(audio_file)
            elif audio_data is None:
                self.logger.error("No audio data or file provided")
                return None
            features = self.extract_features(audio_data)

            if self.use_encryption and self.encryption_manager:
                self.logger.info("Encrypting audio features...")
                encrypted_features = self.encrypt_features(features)

                decrypted_features = self.decrypt_features(encrypted_features)

                if decrypted_features is not None:
                    features = np.array(decrypted_features).reshape(self.n_mfcc, -1)

            recognized_text = self.recognize_speech(audio_data)

            return recognized_text

        except Exception as e:
            self.logger.error(f"Error in secure audio processing: {e}")
            return None

    def start_voice_command_mode(self, chatbot_engine, duration=5):

        self.logger.info("Starting voice command mode...")
        print("Listening for voice commands. Speak now...")

        try:
            with sr.Microphone() as source:
                self.recognizer.adjust_for_ambient_noise(source, duration=1)

                while True:
                    print("\nListening... (say 'exit' to quit)")

                    audio = self.recognizer.listen(source, timeout=duration)

                    try:
                        command = self.recognizer.recognize_google(audio)
                        print(f"You said: {command}")

                        if command.lower() in ["exit", "quit", "goodbye", "bye"]:
                            print("Exiting voice command mode.")
                            break

                        if chatbot_engine:
                            result = chatbot_engine.process_user_input(command)
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