
import os
import logging
import tempfile
from flask import request, jsonify
from werkzeug.utils import secure_filename


class SpeechRoutes:

    def __init__(self, app, speech_recognition=None):
        self.logger = logging.getLogger(__name__)
        self.app = app
        self.speech_recognition = speech_recognition

        self.setup_routes()

    def setup_routes(self):

        @self.app.route('/api/speech_recognition', methods=['POST'])
        def speech_recognition():
            try:

                if not self.speech_recognition:
                    return jsonify({
                        "error": "Speech recognition not configured",
                        "transcript": None
                    }), 500

                audio_data = None
                temp_file = None

                if 'audio' in request.files:
                    audio_file = request.files['audio']
                    if audio_file.filename == '':
                        return jsonify({
                            "error": "No selected file",
                            "transcript": None
                        }), 400

                    audio_filename = secure_filename(audio_file.filename)
                    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(audio_filename)[1])
                    audio_file.save(temp_file.name)
                    temp_file.close()

                    recognized_text = self.speech_recognition.secure_process_audio(audio_file=temp_file.name)

                    os.unlink(temp_file.name)

                elif request.json and 'audio_data' in request.json:
                    import base64
                    import numpy as np

                    audio_base64 = request.json['audio_data']
                    audio_bytes = base64.b64decode(audio_base64)

                    audio_data = np.frombuffer(audio_bytes, dtype=np.int16)

                    recognized_text = self.speech_recognition.secure_process_audio(audio_data=audio_data)

                else:
                    return jsonify({
                        "error": "No audio data provided",
                        "transcript": None
                    }), 400

                if recognized_text:
                    return jsonify({
                        "transcript": recognized_text,
                        "auto_send": request.json.get('auto_send', False) if request.json else False
                    })
                else:
                    return jsonify({
                        "error": "Could not recognize speech",
                        "transcript": None
                    }), 422

            except Exception as e:
                self.logger.error(f"Error processing speech: {e}")
                return jsonify({
                    "error": f"Error processing speech: {str(e)}",
                    "transcript": None
                }), 500

        @self.app.route('/api/speech_status', methods=['GET'])
        def speech_status():

            try:
                status = {
                    "available": self.speech_recognition is not None,
                    "encryption_enabled": (self.speech_recognition is not None and
                                           self.speech_recognition.use_encryption)
                }

                return jsonify(status)

            except Exception as e:
                self.logger.error(f"Error checking speech status: {e}")
                return jsonify({
                    "error": f"Error checking speech status: {str(e)}",
                    "available": False
                }), 500