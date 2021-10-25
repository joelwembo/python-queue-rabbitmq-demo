
# pip install -U Celery
from celery import Celery

app = Celery('hello', broker='amqp://guest@localhost//')

@app.task
def hello():
    return 'hello world'
  
  # $ pip install "celery[librabbitmq]"

# pip install "celery[librabbitmq,redis,auth,msgpack]"
  
