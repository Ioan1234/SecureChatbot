import sys
print(sys.executable)  # Prints the Python interpreter path being used

try:
    import librosa
    print("librosa installed:", librosa.__version__)
except ImportError:
    print("librosa not installed")

try:
    import soundfile
    print("soundfile installed:", soundfile.__version__)
except ImportError:
    print("soundfile not installed")

try:
    import speech_recognition
    print("speech_recognition installed:", speech_recognition.__version__)
except ImportError:
    print("speech_recognition not installed")

import subprocess
import sys

def install_pydub():
    try:
        import pydub
        print("pydub is already installed.")
    except ImportError:
        print("pydub is not installed. Installing now...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pydub"])
        print("pydub installed successfully.")

if __name__ == "__main__":
    install_pydub()
