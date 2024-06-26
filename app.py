from fauna import fql, Document
from fauna.client import Client
from dotenv import load_dotenv
from flask import Flask, Response
import os
import threading

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

# Initialize Fauna client
client = Client(secret=os.getenv("FAUNA_SECRET"), endpoint=os.getenv("FAUNA_ENDPOINT"))
thread = None

def create_app():
  app = Flask(__name__)

  @app.route('/')
  def index():
    return Response("Hello, World!", status=200)

  return app

# Function to run the event streaming in a separate thread
def run_stream_thread():
  print("Function to start thread")
  try:
    global thread
    # Create a new thread to run the stream_events function
    if thread is None:
      thread = threading.Thread(target=stream_events)
      thread.start()
    else:
      print("Thread is already running")
  except Exception as e:
    print(f"Error: {e}")


# Function to stream events
def stream_events():
  with client.stream(fql('Product.all().toStream()')) as stream:
    for event in stream:
      # Print the events directly to the terminal with a newline
      type = event.get('type')
      data = event.get('data')
      print(f"Type: {type}")
      print(f"Data: {data.id}")
      record = {
        "objectID": data.id,
        "name": data.get("name"),
      }
      if (type == "add"):
        print("Add Event")
      if (type == "remove"):
        print("Remove Event")
      if (type == "update"):
        print("Update Event")
      

# Start the event streaming in a separate thread
if __name__ == '__main__':
  app = create_app()
  run_stream_thread()
  app.run(threaded=True)
