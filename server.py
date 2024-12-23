from flask import Flask
import subprocess
import threading
import os

app = Flask(__name__)

def start_holdedstock_viewer():
    """Function to start Holdedstockviewer.py as a separate process."""
    subprocess.run(["python", "Holdedstockviewer.py"])

@app.route('/')
def index():
    return "Livestock Market Viewer is running!"

if __name__ == '__main__':
    # Start the Holdedstockviewer in a separate thread
    viewer_thread = threading.Thread(target=start_holdedstock_viewer)
    viewer_thread.start()

    # Use the PORT environment variable for the port
    port = int(os.environ.get("PORT", 5000))  # Default to 5000 if PORT not set
    app.run(host='0.0.0.0', port=port)